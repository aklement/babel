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

package babel.prep.corpus;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import babel.content.pages.Page;
import babel.util.language.Language;

public class CorpusGenMapper extends MapReduceBase implements Mapper<Text, Page, Text, Page>
{
  protected final static String NO_LANG = "none";
  
  @Override
  public void map(Text url, Page page, OutputCollector<Text, Page> output, Reporter reporter) throws IOException
  {
    Language lang = page.getLanguage();
    output.collect(new Text((lang == null) ? NO_LANG : lang.toString()), page);
  }
}