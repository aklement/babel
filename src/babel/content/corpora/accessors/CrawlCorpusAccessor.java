package babel.content.corpora.accessors;

import java.io.File;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.util.misc.FileList;
import babel.util.misc.PrefixSuffixFileNameFilter;

public class CrawlCorpusAccessor extends TemporalCorpusAccessor
{
  protected static final Log LOG = LogFactory.getLog(CrawlCorpusAccessor.class);
  
  protected static final String DEFAULT_CHARSET = "UTF-8";
  
  /**
   * @param corpusDir
   * @param charset
   * @param from start date (inclusive)
   * @param to end date (exclusive)
   */
  public CrawlCorpusAccessor(String corpusDir, String charset, Date from, Date to, boolean oneSentPerLine)
  {
    super(oneSentPerLine);
    
    if (from == null || to == null || from.after(to))
    { throw new IllegalArgumentException("Dates missing / bad");
    }

    (m_fromDate = new GregorianCalendar()).setTime(from);
    (m_toDate = new GregorianCalendar()).setTime(to);

    resetDays();
    resetFiles();
    
    m_encoding = charset;    
    m_files = new FileList(corpusDir);
  }

  public CrawlCorpusAccessor(String corpusDir, Date from, Date to, boolean oneSentPerLine)
  { 
    this(corpusDir, DEFAULT_CHARSET, from, to, oneSentPerLine);
  }
  
  /**
   * Concatenates all files in the requested stream into a single input stream. 
   * Files appear in no particular order.
   */
  public InputStreamReader getCorpusReader()
  {
    InputStreamReader retReader = null;

    m_files.gather(2);
    
    try
    { retReader = new InputStreamReader(new SequenceInputStream(m_files), m_encoding);
    }
    catch(Exception e)
    { throw new IllegalStateException(e.toString());
    }
    
    return retReader;
  }

  public boolean resetDays()
  {
    m_curDate = null;
    return true;
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
  
  /**
   * @return true iff corpus contains more files.
   */
  public boolean nextFile()
  {
    boolean hasNext = false;
    
    if (m_curFileNum == -1)
    {
      // Accept files with all prefixes
      m_files.gather(2);
      
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
  
  public InputStreamReader getCurDayReader()
  {
    InputStreamReader retReader = null;
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-M-d");

    
    StringBuilder yearSubdir = new StringBuilder(m_files.getDir());
    if (yearSubdir.length() > 0 && !File.separator.equals(yearSubdir.substring(yearSubdir.length() - 1)))
    { yearSubdir.append(File.separator);
    }
    
    yearSubdir.append(m_curDate.get(Calendar.YEAR));
    yearSubdir.append(File.separator);
        
    PrefixSuffixFileNameFilter nameFilter = new PrefixSuffixFileNameFilter(sdf.format(m_curDate.getTime()), null);
    FileList curDayFiles = new FileList(yearSubdir.toString(), nameFilter);
    
    curDayFiles.gather();
    
    try
    { retReader = new InputStreamReader(new SequenceInputStream(curDayFiles), m_encoding);
    }
    catch(Exception e)
    { throw new IllegalStateException(e.toString());
    }
    
    return retReader;
  }
  
  public Date getCurDay()
  {
    return m_curDate.getTime();
  }
  
  /**
   * Advances current date to the next day (or initializes it to start date if 
   * called for the first time.
   * 
   * @return true if success, false if no more days left.
   */
  public boolean nextDay()
  {
    boolean success = true;

    if (m_curDate == null)
    {
      // Set the current date to the from date
      m_curDate = new GregorianCalendar(m_fromDate.get(Calendar.YEAR), m_fromDate.get(Calendar.MONTH), m_fromDate.get(Calendar.DATE));
    }
    else
    {
      // Advance the current date by one day
      m_curDate.add(Calendar.DATE, 1);
      
      success = m_curDate.before(m_toDate);
    }
    
    return success;
  }

  protected GregorianCalendar m_fromDate;
  protected GregorianCalendar m_toDate;
  protected GregorianCalendar m_curDate;
  
  protected int m_curFileNum;
  
  protected String m_encoding;
  
  protected FileList m_files;
}
