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
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import babel.content.pages.Page;
import babel.prep.PrepStep;

/**
 * Fills in content language information for pages lacking it.
 */
public class LangIdentifier extends PrepStep
{
  static final Log LOG = LogFactory.getLog(LangIdentifier.class);
  
  /** Subdirectory of a nutch crawl directory where extracted pages are stored. */
  protected static final String PAGES_SUBDIR = "pages";
  protected static final String JOB_PROP_JOB_REFERRER = "langidentifier.referrer";
  
  public LangIdentifier() throws Exception 
  { super();
  }
  
  /**
   * Configures a map-only language id job.
   */
  protected JobConf createJobConf(String crawlDir, String pagesSubDir, String referrer) throws IOException 
  {    
    JobConf job = new JobConf(getConf());
    job.setJobName("identify languages for pages in " + pagesSubDir);
    
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(LangIdMapper.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);

    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDir));
    
    Path outDir = new Path(new Path(crawlDir, PAGES_SUBDIR), "pages.langid." + getCurTimeStamp());    
    m_fs.delete(outDir, true);

    FileOutputFormat.setOutputPath(job, outDir);

    setUniqueTempDir(job);
    
    job.set(JOB_PROP_JOB_REFERRER, referrer);
    
    return job;
  }
  
  public static void main(String[] args) throws Exception 
  {    
    if (args.length != 3)
    {
      usage();
      return;
    }
    
    LangIdentifier identifier = new LangIdentifier();
    JobConf job = identifier.createJobConf(args[0], args[1], args[2]);

    if (LOG.isInfoEnabled())
    { LOG.info("LangIdentifier: " + job.getJobName());
    }
    
    identifier.runPrepStep(job);
    
    if (LOG.isInfoEnabled()) 
    { 
      LOG.info(Stats.dumpStats() + "\n");
      LOG.info("Output: "+ FileOutputFormat.getOutputPath(job));
      LOG.info("LangIdentifier: done");
    }
  }
  
  protected static void usage()
  {
    System.err.println("Usage: LangIdentifier crawl_dir pages_subdir referrer_url\n");
  }

  /**
   * Keeps track of simple corpus statistics.
   */
  static class Stats
  {
    private static HashMap<String, Integer> langCounts; 
    private static int oldLangCount;
    private static int failedCount;

    static
    {
      langCounts = new HashMap<String, Integer>();
      oldLangCount = 0;
      failedCount = 0;
    }
    
    public synchronized static void incLangPageCount(String lang)
    {
      int curCount = langCounts.containsKey(lang) ? langCounts.get(lang).intValue() + 1 : 1;
      langCounts.put(lang, curCount);      
    }
    
    public synchronized static void incOldLangPageCount()
    {
      oldLangCount++;
    }

    public synchronized static void incFailedCount()
    {
      failedCount++;
    }
    
    public static String dumpStats()
    {
      StringBuilder strBld = new StringBuilder();
      int totalCount = 0;
      int count;
      
      for (String lang : langCounts.keySet())
      {
        strBld.append(lang + " : " + (count = langCounts.get(lang)) + "\n");
        totalCount += count;
      }
      
      strBld.append(totalCount + " pages with added lang info\n");     
      strBld.append(oldLangCount + " pages with existing lang info\n");     
      strBld.append(failedCount + " pages failed detection");     

      return strBld.toString();
    }
  } 
}