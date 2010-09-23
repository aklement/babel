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

package babel.prep.langidtime;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.Page;
import babel.content.pages.PageVersion;

import babel.prep.langidtime.URLAndContentsLangTimeExtractor.DetectionResult;
import babel.util.language.Language;

public class LangAndTimeMapper extends MapReduceBase implements Mapper<Text, Page, Text, Page>
{
  static final Log LOG = LogFactory.getLog(LangAndTimeMapper.class);
  
  /**
   * Sets up the language detector.
   */
  public void configure(JobConf job)
  {
    String referrer = job.get(LangAndTimeExtractor.JOB_PROP_JOB_REFERRER);
    
    m_extractor = new URLAndContentsLangTimeExtractor(referrer);
  }
  
  @Override
  public void map(Text url, Page page, OutputCollector<Text, Page> output, Reporter reporter) throws IOException
  {
    detectAndSetLangTime(page);
    output.collect(url, page);
  }
  
  public void detectAndSetLangTime(Page page)
  {
    DetectionResult result = m_extractor.detect(page);

    // Take care of the language
    Language pageLang = null;
    Language newLang = (result == null) ? null : result.m_langDet.language();
    Language oldLang = page.getLanguage();
    
    if (oldLang != null)
    {
      // Keep the page language unchanged
      if (newLang != null && !oldLang.equals(newLang))
      { LOG.warn("Detected language " + newLang + " conflicts with old language " + oldLang + " for page " + page.pageURL() + ", not changing.");
      }
      pageLang = oldLang;
      LangAndTimeExtractor.Stats.incLangPageCount(oldLang.toString(), page);
    }
    else if (newLang != null)
    {
      // Set the new language
      page.setLanguage(newLang);
      pageLang = newLang;
      LangAndTimeExtractor.Stats.incLangPageCount(newLang.toString(), page);
      LangAndTimeExtractor.Stats.incNewLangPageCount(newLang.toString(), page);
    }
    else
    {
      LangAndTimeExtractor.Stats.incFailedLangPageCount(page);
    }
    
    // Take care of the time (per pageversion)
    Long pageTime, newTime, oldTime;
    
    for (PageVersion ver : page.pageVersions())
    {
     pageTime = null;
     oldTime = ver.getModificationTime();
     if (oldTime == 0)
     { oldTime = null;
     }
     
     newTime = (result != null && result.m_modTimes.containsKey(ver)) ? result.m_modTimes.get(ver).getTime() : null;
     
     if (oldTime != null)
     {
       // Keep the page language unchanged
       if (newTime != null && !oldTime.equals(newTime))
       { LOG.warn("Detected mod time " + new Date(newTime) + " conflicts with old time " + new Date(oldTime) + " for page " + page.pageURL() + ", not changing.");
       }
       pageTime = oldTime;
       
       if (pageLang != null)
       { 
         LangAndTimeExtractor.Stats.incTimeVerCount(pageLang.toString());
       }
     }
     else if (newTime != null)
     {
       ver.setModificationTime(newTime);
       pageTime = newTime;
       
       if (pageLang != null)
       { 
         LangAndTimeExtractor.Stats.incTimeVerCount(pageLang.toString());
         LangAndTimeExtractor.Stats.incNewTimeVerCount(pageLang.toString());
       }
     }
     else if (pageLang != null)
     {
       LangAndTimeExtractor.Stats.incFailedTimeVerCount();
     }

     LOG.info("PageVersion " + page.pageURL() + (pageLang != null ? " Language = " + pageLang : "") + (pageTime != null ? " Time = " + new Date(pageTime) : ""));
    }
  }
    
  private URLAndContentsLangTimeExtractor m_extractor;
}