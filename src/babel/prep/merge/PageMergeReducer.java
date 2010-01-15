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

package babel.prep.merge;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.Page;

public class PageMergeReducer extends MapReduceBase implements Reducer<Text, Page, Text, Page>
{
  public void reduce(Text key, Iterator<Page> pages, OutputCollector<Text, Page> output, Reporter reporter) throws IOException
  {
    Page newPage = new Page(key.toString());
    int numPages = 0;
    
    while (pages.hasNext())
    {
      newPage.merge(pages.next());
      numPages++;
    }
    
    PageMerger.Stats.incPageCount();
    
    if (numPages > 1)
    { PageMerger.Stats.incMergedPageCount();
    }
    
    output.collect(key, newPage);
  }
}