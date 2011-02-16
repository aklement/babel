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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Calendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.MultipleOutputFormat;
import org.apache.hadoop.util.Progressable;

public class DatedLangFilesOutputFormat extends MultipleOutputFormat<Text, Text> 
{
  static final Log LOG = LogFactory.getLog(DatedLangFilesOutputFormat.class);

  static final String DEFAULT_CHARSET = "UTF-8";
  static final String REJECTED_FILE = "rejected.txt";
  static final String EXTENSION = ".txt";
  
  protected String generateFileNameForKeyValue(Text key, Text ver, String name)
  { 
    String toks[] = key.toString().split(DatedCorpusGenMapper.DATE_LANG_SEP);
    
    if (toks == null || toks.length != 2)
    {
      return REJECTED_FILE;
    }
    else
    {
      Calendar cal = Calendar.getInstance();
      cal.setTimeInMillis(Long.parseLong(toks[1]));
      
      int year = cal.get(Calendar.YEAR);
      int month = cal.get(Calendar.MONTH) + 1;
      int day = cal.get(Calendar.DAY_OF_MONTH);
      
      if (year < 2000 || year > 2011)
      {
        return REJECTED_FILE;
      }
      else
      {
        return toks[0] + File.separator + year +  File.separator + year + "-" + month + "-" + day + EXTENSION;
      }
    }    
  }
  
  public RecordWriter<Text, Text> getBaseRecordWriter(final FileSystem fs, JobConf job, String name, final Progressable progress) throws IOException
  {
    final Path dumpFile = new Path(FileOutputFormat.getOutputPath(job), name);

    // Get the old copy out of the way
    if (fs.exists(dumpFile))
    { fs.delete(dumpFile, true);
    }
    else
    { fs.mkdirs(dumpFile.getParent());
    }

    return new RecordWriter<Text, Text>() 
    {
      public synchronized void write(Text key, Text versText) throws IOException 
      { 
        try
        {
          BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(new File(dumpFile.toUri()), true), DEFAULT_CHARSET));
                    
          writer.write(versText.toString());
          writer.close();
        }
        catch (Exception e)
        { throw new RuntimeException("Error writing page versions: " + e.toString());
        }
      }

      public synchronized void close(Reporter reporter) throws IOException 
      { }
    };
  }
}

