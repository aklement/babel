package babel.content.eqclasses.properties.time;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.corpora.accessors.TemporalCorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.PhrasePropertyCollector;

public class PhraseTimeDistributionCollector extends PhrasePropertyCollector {
  
  protected static final Log LOG = LogFactory.getLog(PhraseTimeDistributionCollector.class);

  public PhraseTimeDistributionCollector(int maxPhraseLength, boolean caseSensitive) throws Exception {
    super(maxPhraseLength, caseSensitive);
    
    m_binIdxs = new HashSet<Integer>();
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, Set<? extends EquivalenceClass> phrases) throws Exception {
    
    if (!(corpusAccess instanceof TemporalCorpusAccessor)) {
      throw new IllegalArgumentException("Did not supply a TemporalCorpusAccessor");
    }
   
    TemporalCorpusAccessor temporalAccess = (TemporalCorpusAccessor) corpusAccess;

    // TODO: Inefficient - think of something better
    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>(phrases.size());

    for (EquivalenceClass eq : phrases) {
      for (String word : eq.getAllWords()) { 
        assert eqsMap.get(word) == null;
        eqsMap.put(word, eq);
      }
    }
    
    // Create distribution property objects for all eq
    for (EquivalenceClass ph : phrases) {
      ph.setProperty(new TimeDistribution());
    }
    
    m_binIdxs.clear();
    temporalAccess.resetDays();
    int curDayIdx = 0;
    
    while (temporalAccess.nextDay()) {
      if (recordCurDayCounts(curDayIdx, getCurDayCounts(temporalAccess, eqsMap))) {
        m_binIdxs.add(curDayIdx);
      }
      
      curDayIdx++;
    } 
  }
  
  /**
   * Bin indixes for which counts were collected when collectProperty() was 
   * called the last time.
   */
  public Set<Integer> binsCollected() {
    return m_binIdxs;
  }
  
  /**
   * Records collected counts for a given day.
   */
  protected boolean recordCurDayCounts(int curDayIdx, HashMap<EquivalenceClass, Integer> curDayCounts) {
    
    if ((curDayCounts != null) && (curDayCounts.size() > 0)) {
      for (EquivalenceClass eq : curDayCounts.keySet()) {
        ((TimeDistribution)eq.getProperty(TimeDistribution.class.getName())).addBin(curDayIdx, curDayCounts.get(eq));
      }
    }
    
    return (curDayCounts != null) && (curDayCounts.size() > 0);
  }
  
  /**
   * Get counts from current day files.
   */
  protected HashMap<EquivalenceClass, Integer> getCurDayCounts(TemporalCorpusAccessor temporalAccess, HashMap<String, EquivalenceClass> phMap) throws Exception {
    HashMap<EquivalenceClass, Integer> counts = null;
    String curLine;
    Integer count;
    BufferedReader reader = new BufferedReader(temporalAccess.getCurDayReader());
    EquivalenceClass foundEq;
    List<String> curSents;
    List<IdxPair> sentPhraseIdxs;
    
    if (reader != null && reader.ready()) {
      counts = new HashMap<EquivalenceClass, Integer>();
        
      while ((curLine = reader.readLine()) != null) {
        curLine = curLine.trim();
        
        // Split into likely sentences
        curSents = getSentences(curLine, temporalAccess.isOneSentencePerLine());
        
        // Within each sentence, look for phrases
        for (String sent : curSents) {
   
          // Get all phrases up to length m_maxPhraseLength
          sentPhraseIdxs = getAllPhraseIdxs(sent, m_maxPhraseLength);
          
          for (IdxPair phraseIdx : sentPhraseIdxs) {
                                  
            // Look for the phrase's equivalence class
            if (null != (foundEq = phMap.get(EquivalenceClass.getWordOfAppropriateForm(sent.substring(phraseIdx.from, phraseIdx.to), m_caseSensitive)))) {
              count = counts.get(foundEq);
              counts.put(foundEq, count == null ? 1 : count + 1);
            }
            else {
              // TODO: Any reason not to look for longer phrases?
              //break;
            }
          }
        }
      }
        
      reader.close();
    }
    
    return counts;
  }
  
  protected HashSet<Integer> m_binIdxs;
}
