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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import org.apache.nutch.crawl.CrawlDatum;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseText;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.util.HadoopFSUtil;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.util.NutchJob;

import babel.content.pages.Page;

import babel.prep.PrepStep;

/**
 * Extracts page information for all successfully fetched and parsed pages in
 * all segments of a nutch crawl database.
 */
public class NutchPageExtractor extends PrepStep
{
  protected static final Log LOG = LogFactory.getLog(NutchPageExtractor.class);

  /** Property added to extraction job, so  mappers can recover segment info. */
  static final String JOB_PROP_SEGMENTS_DIR = "pageextractor.segments.dir";
  /** Time when the job began running. */
  protected static final String JOB_PROP_JOB_TIMESTAMP = "pageextractor.timestamp";
  /** Subdirectory of a nutch crawl directory where extracted pages are stored. */
  protected static final String PAGES_SUBDIR = "pages";
  /** Subdirectory of a nutch crawl directory containing nutch segments. */
  protected static final String SEGMENTS_SUBDIR = "segments";

  public NutchPageExtractor() throws Exception
  {
    super(NutchConfiguration.create());
    
    // Page details to extract (see descriptions of the corresponding fields) 
    m_co = true;
    m_fe = true; 
    m_ge = true; 
    m_pa = true; 
    m_pd = true;
    m_pt = true;
  }

  public void configure(JobConf job) throws Exception
  {
    super.configure(job);
    
    m_co = getConf().getBoolean("segment.reader.co", true);
    m_fe = getConf().getBoolean("segment.reader.fe", true);
    m_ge = getConf().getBoolean("segment.reader.ge", true);
    m_pa = getConf().getBoolean("segment.reader.pa", true);
    m_pd = getConf().getBoolean("segment.reader.pd", true);
    m_pt = getConf().getBoolean("segment.reader.pt", true);    
  }
  
  public static void main(String[] args) throws Exception 
  {
    if (args.length != 1)
    {
      usage();
      return;
    }
    
    NutchPageExtractor extractor = new NutchPageExtractor();
    JobConf job = extractor.createJobConf(args[0]);
    
    if (LOG.isInfoEnabled())
    { LOG.info("NutchPageExtractor: " + job.getJobName());
    }
    
    extractor.runPrepStep(job);
    
    if (LOG.isInfoEnabled()) 
    { 
      LOG.info(Stats.dumpStats() + "\n");
      LOG.info("Output: "+ FileOutputFormat.getOutputPath(job));
      LOG.info("NutchPageExtractor: done");
    }
  }
  
  /**
   * Configures the extraction job.
   */
  protected JobConf createJobConf(String crawlDir) throws IOException 
  {
    Path segmentsPath = new Path(crawlDir, SEGMENTS_SUBDIR);
    
    List<Path> segPaths = allSegmentDirs(segmentsPath);
    StringBuilder allSegNames = new StringBuilder();
    
    for (int i = 0; i < segPaths.size(); i++)
    { allSegNames.append(" " + segPaths.get(i).getName());
    }
    
    String timeStamp = getCurTimeStamp();
    
    JobConf job = new NutchJob(getConf());
    job.setJobName("read segments" + allSegNames.toString());

    // Specify what info to extract
    job.setBoolean("segment.reader.co", m_co);
    job.setBoolean("segment.reader.fe", m_fe);
    job.setBoolean("segment.reader.ge", m_ge);
    job.setBoolean("segment.reader.pa", m_pa);
    job.setBoolean("segment.reader.pd", m_pd);
    job.setBoolean("segment.reader.pt", m_pt);
    
    // Specify the paths to extract from for each segment
    for (int i = 0; i < segPaths.size(); i++)
    {
      if (m_ge) FileInputFormat.addInputPath(job, new Path(segPaths.get(i), CrawlDatum.GENERATE_DIR_NAME));
      if (m_fe) FileInputFormat.addInputPath(job, new Path(segPaths.get(i), CrawlDatum.FETCH_DIR_NAME));
      if (m_pa) FileInputFormat.addInputPath(job, new Path(segPaths.get(i), CrawlDatum.PARSE_DIR_NAME));
      if (m_co) FileInputFormat.addInputPath(job, new Path(segPaths.get(i), Content.DIR_NAME));
      if (m_pd) FileInputFormat.addInputPath(job, new Path(segPaths.get(i), ParseData.DIR_NAME));
      if (m_pt) FileInputFormat.addInputPath(job, new Path(segPaths.get(i), ParseText.DIR_NAME));
    }

    // Specify the segments directory so that mapper can recover segment info
    job.set(JOB_PROP_SEGMENTS_DIR, segmentsPath.getName());
    // Store the start time/date of this job
    job.set(JOB_PROP_JOB_TIMESTAMP, timeStamp);

    job.setInputFormat(SequenceFileInputFormat.class);
    job.setMapperClass(PageExtMapper.class);
    job.setReducerClass(PageExtReducer.class);
    
    job.setMapOutputValueClass(NutchChunk.class);
    job.setOutputFormat(SequenceFileOutputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Page.class);

    Path outDir = new Path(new Path(crawlDir, PAGES_SUBDIR), "pages.extract." + timeStamp);    
    m_fs.delete(outDir, true);
    
    FileOutputFormat.setOutputPath(job, outDir);

    setUniqueTempDir(job);

    return job;
  }
  
  /**
   * Enumerates all segment directories.
   */
  protected List<Path> allSegmentDirs(Path segmentsDir) throws IOException
  {
    ArrayList<Path> dirs = new ArrayList<Path>();
    FileStatus[] fstats = m_fs.listStatus(segmentsDir, HadoopFSUtil.getPassDirectoriesFilter(m_fs));
    Path[] files = HadoopFSUtil.getPaths(fstats);
    
    if (files != null && files.length > 0) 
    { dirs.addAll(Arrays.asList(files));
    }
    
    return dirs;
  }
  
  protected static void usage()
  {
    System.err.println("Usage: NutchPageExtractor crawl_dir\n");
  }
  
  /** Includes/skips segment/crawl_generate, which names a set of urls to be fetched. */
  protected boolean m_ge;
  /** Includes/skips segment/crawl_fetch, which contains the status of fetching each url. */
  protected boolean m_fe;
  /** Includes/skips segment/parse_data, which contains outlinks and metadata parsed from each url. */
  protected boolean m_pa;
  /** Includes/skips segment/content, which contains the content of each url.*/
  protected boolean m_co;
  /** Includes/skips segment/parse_data, which contains outlinks and metadata parsed from each url. */
  protected boolean m_pd;
  /** Includes/skips segment/parse_text, which contains the parsed text of each url. */
  protected boolean m_pt;
  
  /**
   * Keeps track of simple page statistics.
   */
  static class Stats
  {
    private static int numPages; 
    private static int numIgnoredPages; 
    private static int numVersions; 
    
    static
    { 
      numPages = 0;
      numVersions = 0;
      numIgnoredPages = 0;
    }
    
    public synchronized static void incPages()
    { 
      numPages++;
    }

    public synchronized static void incIgnoredPages()
    { 
      numIgnoredPages++;
    }
    
    public synchronized static void incVersions()
    {
      numVersions++;
    }

    public synchronized static void incVersions(int inc)
    {
      numVersions += inc;
    }
    
    public static String dumpStats()
    {
      StringBuilder strBld = new StringBuilder();
      
      strBld.append("Extracted pages = " + numPages + " (" + numVersions + " versions)\n");
      strBld.append("Ignored pages = " + numIgnoredPages);
      
      return strBld.toString();
    }
  }
}
