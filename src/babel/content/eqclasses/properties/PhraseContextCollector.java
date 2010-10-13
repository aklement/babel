package babel.content.eqclasses.properties;

import java.io.BufferedReader;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;

public class PhraseContextCollector extends PhrasePropertyCollector {
  
  public static final Log LOG = LogFactory.getLog(PhraseContextCollector.class);
  
  public PhraseContextCollector(int maxPhraseLength, boolean caseSensitive, int leftSize, int rightSize, Set<EquivalenceClass> contextEqs) throws Exception {
    
    super(maxPhraseLength, caseSensitive);
    
    m_leftSize = leftSize;
    m_rightSize = rightSize;
    m_allContextEqsMap = new HashMap<String, EquivalenceClass>(contextEqs.size());
    
    for (EquivalenceClass eq : contextEqs) {
      for (String word : eq.getAllWords()) { 
        assert m_allContextEqsMap.get(word) == null;
        m_allContextEqsMap.put(word, eq);
      }
    }
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> phrases) throws Exception {
    
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    List<String> curSents;
    EquivalenceClass foundEq;
    List<IdxPair> sentPhraseIdxs;
    Context fountEqContext;
    String[] context;
    
    // TODO: Very inefficient - think of something better
    HashMap<String, EquivalenceClass> phMap = new HashMap<String, EquivalenceClass>(phrases.size());
 
    for (EquivalenceClass eq : phrases) {
      phMap.put(eq.getStem(), eq);
    }

    while ((curLine = reader.readLine()) != null) {
      curLine = curLine.trim();
    
      // Split into likely sentences
      curSents = getSentences(curLine, corpusAccess.isOneSentencePerLine());
      
      // Within each sentence, look for phrases
      for (String sent : curSents) {
 
        // Get all phrases up to length m_maxPhraseLength
        sentPhraseIdxs = getAllPhraseIdxs(sent, m_maxPhraseLength);
        
        for (IdxPair phraseIdx : sentPhraseIdxs) {
                                
          // Look for the phrase's equivalence class
          if (null != (foundEq = phMap.get(EquivalenceClass.getWordOfAppropriateForm(sent.substring(phraseIdx.from, phraseIdx.to), m_caseSensitive)))) {
              
            // Get/set its context prop
            if ((fountEqContext = (Context)foundEq.getProperty(Context.class.getName())) == null) {
              foundEq.setProperty(fountEqContext = new Context(foundEq));
            }
     
            // left context
            context = sent.substring(0, phraseIdx.from).split(WORD_DELIM_REGEX);
            for (int idx = context.length - 1; idx >= Math.max(0, context.length - m_leftSize); idx--) {
              // Add current word to the current equivalence class context
              fountEqContext.addContextWord(m_caseSensitive, m_allContextEqsMap, context[idx]);                
            }
              
            // right context
            context = sent.substring(phraseIdx.to, sent.length()).split(WORD_DELIM_REGEX);
            for (int idx = 0; idx < Math.min(m_rightSize, context.length); idx++) {
              // Add current word to the current equivalence class context
              fountEqContext.addContextWord(m_caseSensitive, m_allContextEqsMap, context[idx]);                
            }
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
 
  /** All equivalence classes which from which to construct context. */
  protected HashMap<String, EquivalenceClass> m_allContextEqsMap;
  protected int m_leftSize;
  protected int m_rightSize;
}
