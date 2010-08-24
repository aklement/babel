package babel.content.corpora.accessors;

import java.io.InputStreamReader;

/**
 * Provides access to entire or per-file view of the corpus.
 */
public abstract class CorpusAccessor
{
  protected CorpusAccessor(boolean oneSentPerLine)
  { m_oneSentPerLine = oneSentPerLine;
  }
  
  /**
   * @return an InputStream reader for a entire view of the corpus.
   */
  public abstract InputStreamReader getCorpusReader();

  public boolean isOneSentencePerLine()
  { return m_oneSentPerLine;
  }
  
  /**
   * Resets the per file view. Note that nextFile() must be called before getting
   * the first per-file stream. 
   * @return true iff reset succeeded.
   */
  public abstract boolean resetFiles();
  
  /**
   * @return true iff corpus contains more files.
   */
  public abstract boolean nextFile();

  /**
   * @return an InputStream reader for a current file view of the corpus.
   */
  public abstract NamedInputStreamReader getCurFileReader();

  protected boolean m_oneSentPerLine;
  
  public class NamedInputStreamReader
  {
    public NamedInputStreamReader(String streamName, InputStreamReader reader)
    {
      m_streamName = streamName;
      m_reader = reader;
    }
    
    public String getSteamName()
    { return m_streamName;
    }
    
    public InputStreamReader getReader()
    { return m_reader;
    }
    
    protected String m_streamName;
    protected InputStreamReader m_reader;
  }
}