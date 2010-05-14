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
public class LangAndTimeExtractor extends PrepStep
{
  static final Log LOG = LogFactory.getLog(LangAndTimeExtractor.class);
  
  /** Subdirectory of a nutch crawl directory where extracted pages are stored. */
  protected static final String PAGES_SUBDIR = "pages";
  
  public LangAndTimeExtractor() throws Exception 
  { super();
  }
  
  /**
   * Configures a map-only language id job.
   */
  protected JobConf createJobConf(String crawlDir, String pagesSubDir) throws IOException 
  {    
    JobConf job = new JobConf(getConf());
    job.setJobName("identify languages and collect time for pages in " + pagesSubDir);
    
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(LangAndTimeMapper.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);

    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDir));
    
    Path outDir = new Path(new Path(crawlDir, PAGES_SUBDIR), "pages.langidtime." + getCurTimeStamp());    
    m_fs.delete(outDir, true);

    FileOutputFormat.setOutputPath(job, outDir);
    
    return job;
  }
  
  public static void main(String[] args) throws Exception 
  {    
    if (args.length != 2)
    {
      usage();
      return;
    }
    
    LangAndTimeExtractor identifier = new LangAndTimeExtractor();
    JobConf job = identifier.createJobConf(args[0], args[1]);

    if (LOG.isInfoEnabled())
    { LOG.info("LangAndTimeExtractor: " + job.getJobName());
    }
    
    identifier.runPrepStep(job);
    
    if (LOG.isInfoEnabled()) 
    { 
      LOG.info(Stats.dumpStats() + "\n");
      LOG.info("Output: "+ FileOutputFormat.getOutputPath(job));
      LOG.info("LangAndTimeExtractor: done");
    }
  }
  
  protected static void usage()
  {
    System.err.println("Usage: LangAndTimeExtractor crawl_dir pages_subdir\n");
  }

  /**
   * Keeps track of simple corpus statistics.
   */
  static class Stats
  {
    private static HashMap<String, Integer> langCounts; 
    private static HashMap<String, Integer> newLangCounts;
    private static int failedLangCount;
    private static HashMap<String, Integer> timeCounts; 
    private static HashMap<String, Integer> newTimeCounts;
    private static int failedTimeCount;
    
    static
    {
      langCounts = new HashMap<String, Integer>();
      newLangCounts = new HashMap<String, Integer>();
      timeCounts = new HashMap<String, Integer>();
      newTimeCounts = new HashMap<String, Integer>();
      failedLangCount = 0;
      failedTimeCount = 0;
    }
    
    public synchronized static void incLangPageCount(String lang)
    {
      int curCount = langCounts.containsKey(lang) ? langCounts.get(lang).intValue() + 1 : 1;
      langCounts.put(lang, curCount);      
    }
    
    public synchronized static void incNewLangPageCount(String lang)
    {
      int curCount = newLangCounts.containsKey(lang) ? newLangCounts.get(lang).intValue() + 1 : 1;
      newLangCounts.put(lang, curCount);      
    }
    
    public synchronized static void incTimeCount(String lang)
    {
      int curCount = timeCounts.containsKey(lang) ? timeCounts.get(lang).intValue() + 1 : 1;
      timeCounts.put(lang, curCount);      
    }

    public synchronized static void incNewTimeCount(String lang)
    {
      int curCount = newTimeCounts.containsKey(lang) ? newTimeCounts.get(lang).intValue() + 1 : 1;
      newTimeCounts.put(lang, curCount);      
    }
    
    public synchronized static void incFailedLangCount()
    {
      failedLangCount++;
    }

    public synchronized static void incFailedTimeCount()
    {
      failedTimeCount++;
    }
    
    public static String dumpStats()
    {
      StringBuilder strBld = new StringBuilder();
      
      strBld.append("Existing counts:\n");
      
      for (String lang : langCounts.keySet())
      { strBld.append(lang + " : " + langCounts.get(lang) + "pages, of which " + timeCounts.get(lang) + " are time stamped.\n");
      }

      strBld.append("\nOf those these were just extracted:\n");
      
      for (String lang : newLangCounts.keySet())
      { strBld.append(lang + " : " + newLangCounts.get(lang) + "pages, of which " + newTimeCounts.get(lang) + " are time stamped.\n");
      }

      strBld.append("\nOther stats:\n");
      
      strBld.append(failedLangCount + " pages with no languages detected.\n");     
      strBld.append(failedTimeCount + " pages with detected language but no timestamp.\n");     

      return strBld.toString();
    }
  } 
}