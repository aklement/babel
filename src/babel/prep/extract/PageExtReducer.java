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

package babel.prep.extract;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.Page;

/**
 * Constructs Pages comprised of PageVersions from chunks returned for a URL.
 */
class PageExtReducer extends MapReduceBase implements Reducer<Text, NutchChunk, Text, Page>
{
  public void reduce(Text key, Iterator<NutchChunk> values, OutputCollector<Text, Page> output, Reporter reporter) throws IOException
  {
    // Create a new page (potentially containing multiple versions)
    Page page = new Page(key.toString(), values);
    int numVersions = page.numVersions();
    
    // Only care about it if we have at least one version
    if (numVersions > 0 && (page.pageURL().length() > 0))// && isBBCEnglish(page))
    { 
      NutchPageExtractor.Stats.incPages();
      NutchPageExtractor.Stats.incVersions(numVersions);

      output.collect(key, page);
    }
    else
    {
      NutchPageExtractor.Stats.incIgnoredPages();
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