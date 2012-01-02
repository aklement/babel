package babel.content.corpora.accessors;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.util.misc.FileList;
import babel.util.misc.RegExFileNameFilter;

public class WikiTempCorpusAccessor extends TemporalCorpusAccessor
{
  protected static final Log LOG = LogFactory.getLog(WikiTempCorpusAccessor.class);
  protected static final String DEFAULT_CHARSET = "UTF-8";

  public WikiTempCorpusAccessor(String fileNameRegEx, String corpusDir, boolean oneSentPerLine)
  {
    this(fileNameRegEx, corpusDir, DEFAULT_CHARSET, oneSentPerLine);
  }
  
  public WikiTempCorpusAccessor(String fileNameRegEx, String corpusDir, String charset, boolean oneSentPerLine)
  {
    super(oneSentPerLine);
    
    resetFiles();
    resetDays();
    
    m_files = new FileList(corpusDir, new RegExFileNameFilter(fileNameRegEx));
    m_encoding = charset;
  }
  
  /**
   * Concatenates all files in the requested stream into a single input stream. 
   * Files appear in lexicographic order.
   */
  public InputStreamReader getCorpusReader()
  {
    InputStreamReader retReader = null;
    
    m_files.gather(2);
    m_files.sort();
    
    LOG.info("Wikipedia Pages: "+(m_files.size()));
    
    try
    { retReader = new InputStreamReader(new SequenceInputStream(m_files), m_encoding);
    }
    catch(Exception e)
    { throw new IllegalStateException(e.toString());
    }
    
    return retReader;
  }

  /**
   * @return an InputStream reader for a current file view of the corpus.
   */
  public NamedInputStreamReader getCurFileReader()
  {
    NamedInputStreamReader namedReader = null;
    String fileName;

    try
    {
      if (m_curFileNum != -1)
      {
        fileName = m_files.getFileName(m_curFileNum);
        int lastSlashIdx = fileName.lastIndexOf('/');
        String fileHandle = (lastSlashIdx >= 0) ? fileName.substring(lastSlashIdx + 1) : fileName;
        
        namedReader = new NamedInputStreamReader(fileHandle, new InputStreamReader(new FileInputStream(fileName), m_encoding));
      }
    }
    catch(Exception e)
    { throw new IllegalStateException(e.toString());
    }
    
    return namedReader;
  }

  /**
   * @return true iff corpus contains more files.
   */
  public boolean nextFile()
  {
    boolean hasNext = false;
    
    if (m_curFileNum == -1)
    {
      m_files.gather(2);
      m_files.sort();
      
      if (hasNext = (m_files.size() > 0))
      { m_curFileNum = 0;
      }
    }
    else if (hasNext = ((m_curFileNum + 1) < m_files.size()))
    { m_curFileNum++;
    }
        
    return hasNext;
  }

  /**
   * Resets the per file view. Note that nextFile() must be called before getting
   * the first per-file stream. 
   * @return true iff reset succeeded.
   */
  public boolean resetFiles()
  {
    m_curFileNum = -1;
    return true;
  }

  protected int m_curFileNum;
  protected String m_encoding;  
  protected FileList m_files;

///Get file list to check that src and trg match
  public FileList getFileList(){
	  m_files.gather(2);
	  m_files.sort();
	  return m_files;
  }
  
  ///No Wikipedia dates
public Date getCurDay() {

	// TODO Auto-generated method stub
	return null;
}

/***
 * Return current file reader
 */
public InputStreamReader getCurDayReader() {
	InputStreamReader retReader = (this.getCurFileReader().getReader());
	return retReader;
}

/***
 * Return true if files left, false if not
 */
public boolean nextDay() {
	return nextFile();	
}

/***
 * Same as reset files
 */
public boolean resetDays() {
	this.resetFiles();
	return false;
}
}