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

package babel.prep.datedcorpus;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.Page;
import babel.content.pages.PageVersion;
import babel.util.language.Language;

public class DatedCorpusGenMapper extends MapReduceBase implements Mapper<Text, Page, Text, PageVersion>
{
  public final static String DATE_LANG_SEP = "-";
  
  @Override
  public void map(Text url, Page page, OutputCollector<Text, PageVersion> output, Reporter reporter) throws IOException
  {
    // Map to language and date
    Language lang = page.getLanguage();
    String content;
        
    if (lang != null) // && isBBCEnglish(page))
    {
      Long modTime;
      
      // Only collect pages with language and date
      for (PageVersion ver : page.pageVersions())
      {
        // For Testing: modTime = ver.getFetchTime();        
        modTime = ver.getModificationTime();
        content = ver.getContent();
        
        if (modTime != null && modTime != 0 && content != null && content.length() > 0)
        {
          output.collect(new Text(new String(lang.toString() + DATE_LANG_SEP + modTime.toString())), ver);
          content = ver.getContent();
          
          DatedCorpusGenerator.Stats.incLangPageVerCount(lang.toString());
          DatedCorpusGenerator.Stats.incLangWordCount(lang.toString(), ver.getContent().split("\\s").length);
        }
      }
    } 
  }
  
  protected boolean isBBCEnglish(Page page) {
    String url = removeProtocolAndPrefix(page.pageURL());
    return (url.matches("^bbc.co.uk/(hi/|low/)?english/.*") || url.matches("^bbc.co.uk/local/.*") || url.matches("^bbc.co.uk/[12]/.*"));
  }
  
  protected String removeProtocolAndPrefix(String url)
  {
    // Strip everything up to first dot, and lowercase
    return url.substring(url.indexOf(".") + 1).toLowerCase();
  }
}