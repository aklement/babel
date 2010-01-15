/**
 * This file is licensed to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package babel.prep.langid;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.MetaData;
import babel.content.pages.Page;
import babel.content.pages.PageVersion;

import babel.util.language.LangDetectionResult;
import babel.util.language.LangDetector;
import babel.util.language.google.GoogleLangDetector;

public class LangIdMapper extends MapReduceBase implements Mapper<Text, Page, Text, Page>
{
  /**
   * Sets up the language detector.
   */
  public void configure(JobConf job)
  {
    String referrer = job.get(LangIdentifier.JOB_PROP_JOB_REFERRER);
    
    // m_detector = new NutchLangDetector(job); -OR-
    m_detector = new GoogleLangDetector(referrer);
  }
  
  @Override
  public void map(Text url, Page page, OutputCollector<Text, Page> output, Reporter reporter) throws IOException
  {
    String lang = page.pageProperties().getFirst(Page.PROP_LANG);
    
    if (lang == null)
    {
      if (null != (lang = detectLang(page)))
      { LangIdentifier.Stats.incLangPageCount(lang);
      }
      else
      { LangIdentifier.Stats.incFailedCount();
      }
    }
    else
    { LangIdentifier.Stats.incOldLangPageCount();
    }
    
    output.collect(url, page);
  }
  
  public String detectLang(Page page)
  {
    String lang = null;
    MetaData pageProps = page.pageProperties();
    LangDetectionResult langResult;
    
    pageProps.remove(Page.PROP_LANG_CONFIDENCE);
    pageProps.remove(Page.PROP_LANG_RELIABLE);
    
    // TODO: May help to be more sophisticated, but for now - grab content of 
    // the first available version and run it through the language identifier    
    try
    {    
      String content;

      for (PageVersion ver : page.pageVersions())
      {
        if ((content = ver.getContent()).length() > 0)
        {
          langResult = m_detector.detect(content);
          
          if (langResult.language() != null)
          {
            pageProps.set(Page.PROP_LANG, lang = langResult.language().toString());
       
            if (langResult.confidence() != null)
            { pageProps.set(Page.PROP_LANG_CONFIDENCE, langResult.confidence().toString());
            }
            
            if (langResult.isReliable() != null)
            { pageProps.set(Page.PROP_LANG_RELIABLE, langResult.isReliable().toString());              
            }            
          }
          
          break;
        }
      }
    }
    catch(Exception e)
    {
      if (LangIdentifier.LOG.isErrorEnabled())
      { LangIdentifier.LOG.error("Failed to detect language for page " + page.pageURL() + ": " + e.toString());
      }
    }
    
    return lang;
  }
  
  private LangDetector m_detector;
}