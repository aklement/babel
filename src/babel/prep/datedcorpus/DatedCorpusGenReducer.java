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
import java.util.HashSet;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.PageVersion;

/**
 * Constructs Pages comprised of PageVersions from chunks returned for a URL.
 */
class DatedCorpusGenReducer extends MapReduceBase implements Reducer<Text, PageVersion, Text, Text>
{
  public void reduce(Text key, Iterator<PageVersion> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
  {
    StringBuilder strBld = new StringBuilder();
    HashSet<String> seenVers = new HashSet<String>();
    String content;
    
    while(values.hasNext())
    {
      content = values.next().getContent().trim();
      
      if (!seenVers.contains(content))
      {
        seenVers.add(content);
        strBld.append(content + "\n\n");
      }
    }

    output.collect(key, new Text(strBld.toString()));
  }
}