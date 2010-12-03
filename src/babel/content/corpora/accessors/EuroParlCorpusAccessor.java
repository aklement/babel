package babel.content.corpora.accessors;

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

public class EuroParlCorpusAccessor extends TemporalCorpusAccessor
{ 
  protected static final Log LOG = LogFactory.getLog(EuroParlCorpusAccessor.class);
  protected static final String FILENAME_PREFIX = "ep-";
  
  protected static final String DEFAULT_CHARSET = "UTF-8";
  
  /**
   * @param corpusDir
   * @param charset
   * @param from start date (incluive)
   * @param to end date (exclusive)
   */
  public EuroParlCorpusAccessor(String corpusDir, String charset, Date from, Date to, boolean oneSentPerLine)
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
    
    m_fileNameFilter = new PrefixSuffixFileNameFilter();
    m_files = new FileList(corpusDir, m_fileNameFilter);    
  }

  public EuroParlCorpusAccessor(String corpusDir, Date from, Date to, boolean oneSentPerLine)
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
		
		// Accept files with all prefixes
		m_fileNameFilter.setPrefix(null);
		m_files.gather();
		
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
  	  m_fileNameFilter.setPrefix(null);
  		m_files.gather();
  		
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
		SimpleDateFormat sdf = new SimpleDateFormat("yy-MM-dd");
   
		// Accept files with prefixes indicating current day
    m_fileNameFilter.setPrefix(FILENAME_PREFIX + sdf.format(m_curDate.getTime()));
		m_files.gather();
		
		try
		{ retReader = new InputStreamReader(new SequenceInputStream(m_files), m_encoding);
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
  protected PrefixSuffixFileNameFilter m_fileNameFilter;
}
