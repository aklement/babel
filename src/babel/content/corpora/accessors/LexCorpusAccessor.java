package babel.content.corpora.accessors;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;

import babel.util.misc.FileList;
import babel.util.misc.RegExFileNameFilter;

/**
 * A simple corpus accessor - reads corpus files in lexicographic order.
 */
public class LexCorpusAccessor extends CorpusAccessor
{
  protected static final String DEFAULT_CHARSET = "UTF-8";

  public LexCorpusAccessor(String fileNameRegEx, String corpusDir, boolean oneSentPerLine)
  {
    this(fileNameRegEx, corpusDir, DEFAULT_CHARSET, oneSentPerLine);
  }
  
  public LexCorpusAccessor(String fileNameRegEx, String corpusDir, String charset, boolean oneSentPerLine)
  {
    super(oneSentPerLine);
    
    resetFiles();
    
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
}