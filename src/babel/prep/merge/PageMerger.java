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
 * Merges new pages and existing pages.
 */
public class PageMerger extends PrepStep
{
  static final Log LOG = LogFactory.getLog(PageMerger.class);
  
  /** Subdirectory of a nutch crawl directory where extracted pages are stored. */
  protected static final String PAGES_SUBDIR = "pages";
  
  public PageMerger() throws Exception 
  { super();
  }
  
  /**
   * Configures a reduce-only page merge job.
   */
  protected JobConf createJobConf(String crawlDir, String pagesSubDirOne, String pagesSubDirTwo) throws IOException 
  {    
    JobConf job = new JobConf(getConf());
    job.setJobName("merge pages in " + pagesSubDirOne + " and " + pagesSubDirTwo);
    
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setReducerClass(PageMergeReducer.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);

    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDirOne));
    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDirTwo));
    
    Path outDir = new Path(new Path(crawlDir, PAGES_SUBDIR), "pages.merge." + getCurTimeStamp());    
    m_fs.delete(outDir, true);

    FileOutputFormat.setOutputPath(job, outDir);
    
    setUniqueTempDir(job);
    
    return job;
  }
  
  public static void main(String[] args) throws Exception 
  {    
    if (args.length != 3)
    {
      usage();
      return;
    }
 
    PageMerger merger = new PageMerger();
    JobConf job = merger.createJobConf(args[0], args[1], args[2]);
    
    if (LOG.isInfoEnabled())
    { LOG.info("PageMerger: " + job.getJobName());
    }
    
    merger.runPrepStep(job);
    
    if (LOG.isInfoEnabled()) 
    { 
      LOG.info(Stats.dumpStats() + "\n");
      LOG.info("Output: "+ FileOutputFormat.getOutputPath(job));
      LOG.info("PageMerger: done");
    }
  }
  
  protected static void usage()
  {
    System.err.println("Usage: PageMerger crawl_dir pages_subdir_1 pages_subdir_2\n");
  }

  /**
   * Keeps track of simple corpus statistics.
   */
  static class Stats
  {
    private static int pageCount;
    private static int mergedPageCount;

    static
    {
      pageCount = 0;
      mergedPageCount = 0;
    }
    
    public synchronized static void incPageCount()
    {
      pageCount++;
    }

    public synchronized static void incMergedPageCount()
    {
      mergedPageCount++;
    }
    
    public static String dumpStats()
    {
      StringBuilder strBld = new StringBuilder();

      strBld.append(pageCount + " pages generated of which ");     
      strBld.append(mergedPageCount + " were merged\n");     

      return strBld.toString();
    }
  }
}