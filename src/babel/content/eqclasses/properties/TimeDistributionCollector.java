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

  public TimeDistributionCollector(boolean caseSensitive) throws Exception
  {
    m_caseSensitive = caseSensitive;
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> eqs) throws Exception
  {    
    if (!(corpusAccess instanceof TemporalCorpusAccessor))
    { throw new IllegalArgumentException("Did not supply a TemporalCorpusAccessor");
    }
   
    TemporalCorpusAccessor temporalAccess = (TemporalCorpusAccessor) corpusAccess;

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
    
    temporalAccess.resetDays();
    int curDayIdx = 0;
    
    while (temporalAccess.nextDay())
    {
      recordCurDayCounts(curDayIdx, getCurDayCounts(temporalAccess, eqsMap));
      curDayIdx++;
    } 
  }
  
  /**
   * Records collected counts for a given day.
   */
  protected void recordCurDayCounts(int curDayIdx, HashMap<EquivalenceClass, Integer> curDayCouns)
  {
    if (curDayCouns != null)
    {
      for (EquivalenceClass eq : curDayCouns.keySet())
      {
        ((TimeDistribution)eq.getProperty(TimeDistribution.class.getName())).addBin(curDayIdx, curDayCouns.get(eq));
      }
    }
  }
  
  /**
   * Get counts from current day files.
   */
  protected HashMap<EquivalenceClass, Integer> getCurDayCounts(TemporalCorpusAccessor temporalAccess, HashMap<String, EquivalenceClass> eqsMap) throws Exception
  {
    HashMap<EquivalenceClass, Integer> counts = null;
    String curLine;
    String[] curTokens;
    Integer count;
    BufferedReader reader = new BufferedReader(temporalAccess.getCurDayReader());
    EquivalenceClass curEq;
    String word;
    
    if (reader != null && reader.ready())
    {
      counts = new HashMap<EquivalenceClass, Integer>();
        
      while ((curLine = reader.readLine()) != null)
      {
        curTokens = curLine.split(WORD_DELIM_REGEX);

        for (int numToken = 0; numToken < curTokens.length; numToken++)
        {
          word = EquivalenceClass.getWordOfAppropriateForm(curTokens[numToken], m_caseSensitive);
          curEq = eqsMap.get(word);
          
          if (curEq != null)
          {
            count = counts.get(curEq);
            counts.put(curEq, count == null ? 1 : count + 1);
          }
        }
      }
        
      reader.close();
    }
    
    return counts;
  }
  
  protected boolean m_caseSensitive;
}
