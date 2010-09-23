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

import javax.xml.stream.XMLStreamException;

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

import babel.content.pages.Page;

import babel.util.language.Language;
import babel.util.persistence.XMLObjectWriter;

/** 
 * Generates multiple XML files each containing pages in a single nalguage.
 */
class MultipleXMLLangFileOutputFormat extends MultipleOutputFormat<Text, Page> 
{
  static final Log LOG = LogFactory.getLog(MultipleXMLLangFileOutputFormat.class);
  
  protected String generateFileNameForKeyValue(Text key, Page page, String name)
  {
    Language lang = page.getLanguage();
    String strLang = (lang == null) ? "none" : lang.toString();
    
    if (LOG.isInfoEnabled())
    { LOG.info("Language " + strLang + " for page " + page.pageURL());
    }
    
    CorpusGenerator.Stats.incLangPageCount(strLang);
    
    return strLang + "." + super.generateFileNameForKeyValue(key, page, name);
  }
  
  public RecordWriter<Text, Page> getBaseRecordWriter(final FileSystem fs, JobConf job, String name, final Progressable progress) throws IOException
  {
    final Path dumpFile = new Path(FileOutputFormat.getOutputPath(job), name);

    // Get the old copy out of the way
    if (fs.exists(dumpFile)) fs.delete(dumpFile, true);

    final XMLObjectWriter xmlWriter;
    
    try
    { xmlWriter = new XMLObjectWriter(fs.create(dumpFile), false);
    }
    catch (Exception e)
    { throw new RuntimeException("Failed to instantiate XMLObjectWriter.");
    }
          
    return new RecordWriter<Text, Page>() 
    {
      public synchronized void write(Text key, Page page) throws IOException 
      { 
        try
        { xmlWriter.write(page);
        }
        catch (XMLStreamException e)
        { throw new RuntimeException("Error writing page XML.");
        }
      }

      public synchronized void close(Reporter reporter) throws IOException 
      { 
        try
        {
          xmlWriter.close();
        }
        catch (XMLStreamException e)
        { throw new RuntimeException("Error closing XMLObjectWriter.");
        }
      }
    };
  }
} 