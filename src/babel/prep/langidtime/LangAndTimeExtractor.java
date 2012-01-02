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
  protected static final String JOB_PROP_JOB_REFERRER = "langidentifier.referrer";
  
  public LangAndTimeExtractor() throws Exception 
  { super();
  }
  
  /**
   * Configures a map-only language id job.
   */
  protected JobConf createJobConf(String crawlDir, String pagesSubDir, String referrer) throws IOException 
  {    
    JobConf job = new JobConf(getConf());
    job.setJobName("identify languages and collect time for pages in " + pagesSubDir);
    
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(LangAndTimeMapper.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);
    
    //ANNI EDIT
    job.setNumMapTasks(2);
    job.setNumReduceTasks(2);
    //END ANNI EDIT
    
    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDir));
    
    Path outDir = new Path(new Path(crawlDir, PAGES_SUBDIR), "pages.langidtime." + getCurTimeStamp());    
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
    
    LangAndTimeExtractor identifier = new LangAndTimeExtractor();
    JobConf job = identifier.createJobConf(args[0], args[1], args[2]);

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
    System.err.println("Usage: LangAndTimeExtractor crawl_dir pages_subdir referrer_url\n");
  }

  /**
   * Keeps track of simple corpus statistics.
   */
  static class Stats
  {
    private static HashMap<String, Integer> pageLangCounts; 
    private static HashMap<String, Integer> pageNewLangCounts;
    private static int pageFailedLangCount;

    private static HashMap<String, Integer> verLangCounts; 
    private static HashMap<String, Integer> verNewLangCounts;
    private static int verFailedLangCount;
    
    private static HashMap<String, Integer> verTimeCounts; 
    private static HashMap<String, Integer> verNewTimeCounts;
    private static int verFailedTimeCount;
    
    static
    {
      pageLangCounts = new HashMap<String, Integer>();
      pageNewLangCounts = new HashMap<String, Integer>();
      pageFailedLangCount = 0;

      verLangCounts = new HashMap<String, Integer>();
      verNewLangCounts = new HashMap<String, Integer>();
      verFailedLangCount = 0;
      
      verTimeCounts = new HashMap<String, Integer>();
      verNewTimeCounts = new HashMap<String, Integer>();
      verFailedTimeCount = 0;
    }
    
    public synchronized static void incLangPageCount(String lang, Page page)
    {
      int curCount = pageLangCounts.containsKey(lang) ? pageLangCounts.get(lang).intValue() + 1 : 1;
      pageLangCounts.put(lang, curCount);      

      curCount = verLangCounts.containsKey(lang) ? verLangCounts.get(lang).intValue() + page.pageVersions().size() : page.pageVersions().size();
      verLangCounts.put(lang, curCount); 
    }
    
    public synchronized static void incNewLangPageCount(String lang, Page page)
    {
      int curCount = pageNewLangCounts.containsKey(lang) ? pageNewLangCounts.get(lang).intValue() + 1 : 1;
      pageNewLangCounts.put(lang, curCount);      

      curCount = verNewLangCounts.containsKey(lang) ? verNewLangCounts.get(lang).intValue() + page.pageVersions().size() : page.pageVersions().size();
      verNewLangCounts.put(lang, curCount);  
    }
    
    public synchronized static void incFailedLangPageCount(Page page)
    {
      pageFailedLangCount++;
      verFailedLangCount += page.pageVersions().size();
    }
    
    public synchronized static void incTimeVerCount(String lang)
    {
      int curCount = verTimeCounts.containsKey(lang) ? verTimeCounts.get(lang).intValue() + 1 : 1;
      verTimeCounts.put(lang, curCount);      
    }

    public synchronized static void incNewTimeVerCount(String lang)
    {
      int curCount = verNewTimeCounts.containsKey(lang) ? verNewTimeCounts.get(lang).intValue() + 1 : 1;
      verNewTimeCounts.put(lang, curCount);      
    }

    public synchronized static void incFailedTimeVerCount()
    {
      verFailedTimeCount++;
    }
    
    public static String dumpStats()
    {
      StringBuilder strBld = new StringBuilder();
      
      strBld.append("Existing counts:\n");
      
      for (String lang : pageLangCounts.keySet())
      { strBld.append(lang + " : " + pageLangCounts.get(lang) + " pages containing " + verLangCounts.get(lang) + " versions of which " + (verTimeCounts.containsKey(lang) ? verTimeCounts.get(lang) : "0") + " are time stamped.\n");
      }

      strBld.append("\nOf those these were just extracted:\n");
      
      for (String lang : pageNewLangCounts.keySet())
      { strBld.append(lang + " : " + pageNewLangCounts.get(lang) + " pages containing " + verNewLangCounts.get(lang) + " versions of which " + (verNewTimeCounts.containsKey(lang) ? verNewTimeCounts.get(lang) : "0") + " are time stamped.\n");
      }

      strBld.append("\nOther stats:\n");
      
      strBld.append(pageFailedLangCount + " pages (" + verFailedLangCount + " versions) with no languages detected.\n");     
      strBld.append(verFailedTimeCount + " page versions with detected language but no timestamp.\n");     

      return strBld.toString();
    }
  } 
}