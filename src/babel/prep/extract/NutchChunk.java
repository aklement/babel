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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.crawl.NutchWritable;

/**
 * Wraps NutchWritable to include a segment from which this chunk was read. It 
 * will be used to reconstruct PageVersions from chunks, and then Pages from
 * PageVersions.
 */
public class NutchChunk extends NutchWritable
{
  public NutchChunk()
  {
    m_segmentId = new String();
  }
  
  public NutchChunk(NutchChunk other)
  {
    this(other.m_segmentId, other.get());
  }
  
  public NutchChunk(String segmentId, Writable instance)
  {
    m_segmentId = segmentId;
    set(instance);
  }
  
  public void readFields(DataInput in) throws IOException
  {
    m_segmentId = Text.readString(in);
    super.readFields(in);
  }

  public void write(DataOutput out) throws IOException
  {
    Text.writeString(out, m_segmentId);
    super.write(out);
  }
  
  public String getSegmentId()
  {
    return m_segmentId;
  }
  
  protected String m_segmentId;
}
