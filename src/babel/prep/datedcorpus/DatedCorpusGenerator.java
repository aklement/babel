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
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import babel.content.pages.PageVersion;
import babel.prep.PrepStep;

/**
 * Reads in pages, and produces a language-split (optionally, XML formatted)
 * dataset.
 */
public class DatedCorpusGenerator extends PrepStep
{
  protected static final Log LOG = LogFactory.getLog(DatedCorpusGenerator.class);
  
  /** Subdirectory of a nutch crawl directory where the generated corpus is stored. */
  protected static final String CORPUS_SUBDIR = "datedcorpus";
  
  public DatedCorpusGenerator() throws Exception 
  { super();
  }
  
  /**
   * Configures a map-only dataset generation job.
   */
  protected JobConf createJobConf(String crawlDir, String pagesSubDir) throws IOException 
  {    
    JobConf job = new JobConf(getConf());
    job.setJobName("create dated dataset from " + pagesSubDir);
    
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(DatedCorpusGenMapper.class);
    job.setReducerClass(DatedCorpusGenReducer.class);
    
    job.setMapOutputValueClass(PageVersion.class);
    job.setOutputFormat(DatedLangFilesOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDir));
    
    Path outDir = new Path(new Path(crawlDir, CORPUS_SUBDIR), "datedcorpus." + getCurTimeStamp());    
    m_fs.delete(outDir, true);

    FileOutputFormat.setOutputPath(job, outDir);

    setUniqueTempDir(job);
    
    return job;
  }
  
  public static void main(String[] args) throws Exception 
  {    
    if (args.length < 2 || args.length > 3)
    {
      usage();
      return;
    }
 
    DatedCorpusGenerator gen = new DatedCorpusGenerator();
    JobConf job = gen.createJobConf(args[0], args[1]);
    
    if (LOG.isInfoEnabled())
    { LOG.info("DatedCorpusGenerator: " + job.getJobName());
    }
    
    gen.runPrepStep(job);
 
    if (LOG.isInfoEnabled()) 
    { 
      LOG.info(Stats.dumpStats() + "\n");
      LOG.info("Output: "+ FileOutputFormat.getOutputPath(job));
      LOG.info("DatedCorpusGenerator: done");
    }    
  }
  
  protected static void usage()
  {
    System.err.println("Usage: DatedCorpusGenerator crawl_dir pages_subdir\n");
  }

  /**
   * Keeps track of simple corpus statistics.
   */
  static class Stats
  {
    private static HashMap<String, Long> pageVerCounts; 
    private static HashMap<String, Long> wordCounts; 
    
    static
    {
      pageVerCounts = new HashMap<String, Long>();
      wordCounts = new HashMap<String, Long>();
    }
    
    public synchronized static void incLangPageVerCount(String lang)
    {
      long curCount = pageVerCounts.containsKey(lang) ? pageVerCounts.get(lang).intValue() + 1 : 1;
      pageVerCounts.put(lang, curCount);      
    }

    public synchronized static void incLangWordCount(String lang, long count)
    {
      long curCount = wordCounts.containsKey(lang) ? wordCounts.get(lang).intValue() + count : count;
      wordCounts.put(lang, curCount);
    }
    
    public static String dumpStats()
    {
      StringBuilder strBld = new StringBuilder();
      int totalCount = 0;
      long count;
      
      for (String lang : pageVerCounts.keySet())
      {
        strBld.append(lang + " : " + (count = pageVerCounts.get(lang)) + ", " + wordCounts.get(lang) + "\n");
        totalCount += count;
      }
      
      strBld.append("Total page versions = " + totalCount);     
      return strBld.toString();
    }
  }
}