package babel.content.corpora.accessors;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.util.regex.Pattern;

import babel.util.config.Configurator;
import babel.util.misc.FileList;

/**
 * A simple corpus accessor - reads corpus files in lexicographic order.
 */
public class LexCorpusAccessor extends CorpusAccessor
{
  protected static final String CORPUS_PATH;
  protected static final String DEFAULT_CHARSET = "UTF-8";
  
  /** Populate missing parameters from configurator. */
  static
  { 
    CORPUS_PATH = Configurator.getResourcePath() + "/" + Configurator.lookup("CORPUS_PATH");
  }

  public LexCorpusAccessor(String fileNameRegEx)
  {
    this(fileNameRegEx, DEFAULT_CHARSET);
  }
  
  public LexCorpusAccessor(String fileNameRegEx, String charset)
  {
    super();
    
    resetFiles();
    
    m_fileNameFilter = new RegExFileNameFilter(fileNameRegEx);
    m_files = new FileList(CORPUS_PATH, m_fileNameFilter);
    m_encoding = charset;
  }
  
  /**
   * Concatenates all files in the requested stream into a single input stream. 
   * Files appear in lexicographic order.
   */
  public InputStreamReader getCorpusReader()
  {
    InputStreamReader retReader = null;
    
    m_files.gather();
    m_files.sort();

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
      m_files.gather();
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
  protected RegExFileNameFilter m_fileNameFilter;
  
  class RegExFileNameFilter implements FilenameFilter
  {

    public RegExFileNameFilter()
    { 
      this(null);
    }
    
    public RegExFileNameFilter(String regEx)
    { 
      super();
      m_regEx = regEx;
    }
    
    public void setRegEx(String regEx)
    {
      m_regEx = regEx;      
    }
    
    public boolean accept(File dir, String name)
    {
      return (m_regEx == null) || Pattern.matches(m_regEx, name);
    }
    
    protected String m_regEx;
  }

}