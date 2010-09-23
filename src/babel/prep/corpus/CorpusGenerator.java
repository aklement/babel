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
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;

import babel.content.pages.Page;
import babel.prep.PrepStep;

/**
 * Reads in pages, and produces a language-split (optionally, XML formatted)
 * dataset.
 */
public class CorpusGenerator extends PrepStep
{
  protected static final Log LOG = LogFactory.getLog(CorpusGenerator.class);

  public static final String PARAM_XML = "xml";
  
  /** Subdirectory of a nutch crawl directory where the generated corpus is stored. */
  protected static final String CORPUS_SUBDIR = "corpus";
  
  public CorpusGenerator() throws Exception 
  { super();
  }
  
  /**
   * Configures a map-only dataset generation job.
   */
  protected JobConf createJobConf(String crawlDir, String pagesSubDir, boolean xmlOut) throws IOException 
  {    
    JobConf job = new JobConf(getConf());
    job.setJobName("create " + (xmlOut ? "xml formatted" : "") + " dataset from " + pagesSubDir);
    
    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(CorpusGenMapper.class);
    job.setOutputFormat(xmlOut ? MultipleXMLLangFileOutputFormat.class : MultipleLangFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);

    FileInputFormat.addInputPath(job, new Path(crawlDir, pagesSubDir));
    
    Path outDir = new Path(new Path(crawlDir, CORPUS_SUBDIR), "corpus." + (xmlOut ? PARAM_XML + "."  : "") + getCurTimeStamp());    
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
 
    CorpusGenerator gen = new CorpusGenerator();
    JobConf job = gen.createJobConf(args[0], args[1], (args.length == 3) && PARAM_XML.equals(args[2]));
    
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
    System.err.println("Usage: DatedCorpusGenerator crawl_dir pages_subdir [" + PARAM_XML + "]\n");
  }

  /**
   * Keeps track of simple corpus statistics.
   */
  static class Stats
  {
    private static HashMap<String, Integer> langCounts; 
    
    static
    {
      langCounts = new HashMap<String, Integer>();
    }
    
    public synchronized static void incLangPageCount(String lang)
    {
      int curCount = langCounts.containsKey(lang) ? langCounts.get(lang).intValue() + 1 : 1;
      langCounts.put(lang, curCount);      
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
      
      strBld.append("Total = " + totalCount);     
      return strBld.toString();
    }
  }
}
