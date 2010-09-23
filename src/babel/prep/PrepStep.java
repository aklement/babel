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

package babel.prep;

import java.io.IOException;
import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

/**
 * To be extended by each class implementing a pre-processing step.
 */
public abstract class PrepStep extends Configured
{
  public PrepStep() throws IOException
  { 
    this(new JobConf(PrepStep.class));
    m_fs = FileSystem.get(getConf());
  }
  
  public PrepStep(Configuration conf) throws IOException
  { 
    super(new JobConf(conf, PrepStep.class));
    m_fs = FileSystem.get(getConf());
  }
  
  public void configure(JobConf job) throws Exception
  {
    setConf(job);
    m_fs = FileSystem.get(getConf());
  }
  
  public void runPrepStep(JobConf job) throws IOException
  {    
    JobClient.runJob(job);
    cleanUpAfterJob(job);
  }
 
  protected void cleanUpAfterJob(JobConf job) throws IOException
  {
    m_fs.close();
  }
  
  protected String getCurTimeStamp()
  {
    return (new SimpleDateFormat("yyMMdd-HHmmss")).format(Calendar.getInstance().getTime());
  }
  
  protected void setUniqueTempDir(JobConf job)
  {
    Path tempDir = new Path(getConf().get("hadoop.tmp.dir", ".") + "/"+ java.util.UUID.randomUUID().toString());
    job.set("hadoop.tmp.dir", tempDir.toString());
  }
  
  /**
   * Dumps the configuration (for debugging only).
   */
  protected String dumpJobProps(JobConf job)
  {
    Iterator<Entry<String, String>> it = job.iterator();
    StringBuilder strBld = new StringBuilder();
    Entry<String, String> cur;
    
    while(it.hasNext())
    {
      cur = (Entry<String, String>)it.next();
      strBld.append(cur.getKey() + " : " + cur.getValue() + "\n");
    }
    
    return strBld.toString();
  }
  
  protected FileSystem m_fs;
}
