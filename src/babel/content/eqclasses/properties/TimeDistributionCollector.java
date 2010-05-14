package babel.content.eqclasses.properties;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.corpora.accessors.TemporalCorpusAccessor;

import babel.content.corpora.accessors.CorpusAccessor;

public class TimeDistributionCollector extends PropertyCollector
{
  protected static final Log LOG = LogFactory.getLog(TimeDistributionCollector.class);
    
  /**
   * @param slidingWindow
   * @param windowSize - number of days in each bin
   * @throws Exception 
   */
  @SuppressWarnings("unchecked")
  public TimeDistributionCollector(boolean caseSensitive, int windowSize, boolean slidingWindow) throws Exception
  {
    m_caseSensitive = caseSensitive;
    m_slidingWindow = slidingWindow;
    m_window = new HashMap[windowSize];    
    m_binIdx = 0;
  }
  
  /**
   * @param corpusAccess Corpus from which to gather the property.
   * @param eq for which to gather properties.
   * @throws Exception 
   * @throws Exception 
   */
  public void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> eqs) throws Exception
  {
    if (!(corpusAccess instanceof TemporalCorpusAccessor))
    { throw new IllegalArgumentException("Did not supply a TemporalCorpusAccessor");
    }
    
    // TODO: Inefficient - think of something better
    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>(eqs.size());

    for (EquivalenceClass eq : eqs)
    {
      for (String word : eq.getAllWords())
      { 
        assert eqsMap.get(word) == null;
        eqsMap.put(word, eq);
      }
    }
    
    // Create distribution property objects for all eq
    for (EquivalenceClass curEq : eqs)
    { curEq.setProperty(new TimeDistribution());
    }
    
    /** Start off by reading the entire window. */
    boolean done = !readEntireWindow((TemporalCorpusAccessor)corpusAccess, eqsMap);
    
    while(!done)
    {
      // Advance one bin (of sliding window) or entire window at a time.
      recordWindow(eqs);
      done = !(m_slidingWindow ? readNextBin((TemporalCorpusAccessor)corpusAccess, eqsMap) : readEntireWindow((TemporalCorpusAccessor)corpusAccess, eqsMap));
    }

    m_binIdx = 0;
  }
  
  /**
   * Records the aggregate count of the whole window it into EquivalenceClass' 
   * time distributions (for each of them).
   */
  protected void recordWindow(Set<? extends EquivalenceClass> eqs)
  {
    int entCount;
    Integer count;
 
    for (EquivalenceClass eq : eqs)
    {
      entCount = 0;
      
      for (String word : eq.getAllWords())
      {      
        for (int curDay = 0; curDay < m_window.length; curDay++)
        {
          count = m_window[curDay].get(word);
          entCount += (count == null) ? 0 : count;
        }
      }
      
      ((TimeDistribution)eq.getProperty(TimeDistribution.class.getName())).addBin(entCount);
    }
  }
  
  /**
   * Reads an entire window worth of bins.
   * @throws Exception 
   */
  protected boolean readEntireWindow(TemporalCorpusAccessor corpusAccess, HashMap<String, EquivalenceClass> eqsMap) throws Exception
  {
    boolean entireWin = true;
    int binIdx = m_binIdx;
    
    do 
    {
      if (!(entireWin = readIntoBin(m_binIdx, corpusAccess, eqsMap)))
      { break;
      }
    } while (nextBinIndex() != binIdx);

    
    return entireWin;
  }

  /**
   * Reads into current bin - if successful, advances to the next bin, 
   * otheriwse moves on to the next day and tries again.
   * @throws Exception 
   */
  protected boolean readNextBin(TemporalCorpusAccessor corpusAccess, HashMap<String, EquivalenceClass> eqsMap) throws Exception
  {
    boolean read = readIntoBin(m_binIdx, corpusAccess, eqsMap);
    
    if (read)
    { nextBinIndex();
    }
    
    return read;
  }
  
  /**
   * Reads current day counts into a window's bin.
   * @throws Exception 
   */
  protected boolean readIntoBin(int binNumber, TemporalCorpusAccessor corpusAccess, HashMap<String, EquivalenceClass> eqsMap) throws Exception
  {
    m_window[binNumber] = null;
    
    // TODO: We are skipping over days with no counts, is that OK?
    while (corpusAccess.nextDay() && (m_window[binNumber] = getCurDayCounts(corpusAccess, eqsMap)) == null);
    
    return (m_window[binNumber] != null);
  }
  
  /**
   * Get counts for eq from the current day files.
   */
  protected HashMap<String, Integer> getCurDayCounts(TemporalCorpusAccessor corpusAccess, HashMap<String, EquivalenceClass> eqsMap) throws Exception
  {
    HashMap<String, Integer> counts = null;
    String curLine;
    String[] curTokens;
    Integer count;
    BufferedReader reader = new BufferedReader(corpusAccess.getCurDayReader());
    String word;
    
    if (reader != null && reader.ready())
    {
      counts = new HashMap<String, Integer>();
        
      while ((curLine = reader.readLine()) != null)
      {
        curTokens = curLine.split(WORD_DELIM_REGEX);

        for (int numToken = 0; numToken < curTokens.length; numToken++)
        {
          word = EquivalenceClass.getWordOfAppropriateForm(curTokens[numToken], m_caseSensitive);
          
          // Is that a word we care about?
          if (eqsMap.containsKey(word))
          {
            count = counts.get(word);
            counts.put(word, count == null ? 1 : count + 1);
          }
        }
      }
        
      reader.close();
    }
    
    return counts;
  }
  
  /**
   * Advances to the next bin index (circular array).
   * @return
   */
  protected int nextBinIndex()
  {
    m_binIdx++;
    
    if (m_binIdx >= m_window.length)
    { m_binIdx = 0;
    }
    
    return m_binIdx;
  }
  
  protected boolean m_caseSensitive;
  /** True iff sliding window. */
  protected boolean m_slidingWindow;
  /** Actual counts: index is a bin in the window. */
  protected HashMap<String, Integer>[] m_window;
  /** Indexes the next available bin. */
  protected int m_binIdx;
}
