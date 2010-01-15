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

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Adds segment info to extracted chunks, so that multiple versions of a page 
 * from multiple segments can be properly assembled. Maps the chunks to page
 * URL.
 */
class PageExtMapper extends MapReduceBase implements Mapper<Text, Writable, Text, NutchChunk>
{ 
  /**
   * Recovers the nutch segment processed by this mapper from job config.
   */
  public void configure(JobConf job) throws IllegalArgumentException
  {
    // Recover the segment processed by this mapper
    String fileDir = job.get("map.input.file");
    String segmentsDir = job.get(NutchPageExtractor.JOB_PROP_SEGMENTS_DIR);
    int idx;
    
    if (fileDir == null || segmentsDir == null || ((idx = fileDir.indexOf(segmentsDir)) < 0))
    { 
      throw new IllegalArgumentException("Could not recover mapper's segment.");
    }
    else
    {
      String segment = fileDir.substring(idx + segmentsDir.length()); 
      idx = segment.startsWith(File.separator) ? 1 : 0;
      m_segmentId = segment.substring(idx, segment.indexOf(File.separator, idx));     
    }
  }
  
  public void map(Text key, Writable value, OutputCollector<Text, NutchChunk> collector, Reporter reporter) throws IOException
  {      
    collector.collect(key, new NutchChunk(m_segmentId, value));
  }
  
  protected String m_segmentId;
}