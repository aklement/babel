package babel.content.eqclasses.properties;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.phrases.Phrase;
import babel.util.misc.GettableHashSet;
import babel.util.misc.InvertibleHashMap;

public class PhraseOrderCollector extends PhrasePropertyCollector {

  public static final Log LOG = LogFactory.getLog(PhrasePropertyCollector.class);
  
  public PhraseOrderCollector(int maxPhraseLength, boolean caseSensitive) {    
    super(maxPhraseLength, caseSensitive);    
  }

  public void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> phrases) throws Exception {
    
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    List<String> curSents;
    PhraseContext phraseContext;
    List<IndexedPhrase> sentPhrases;
    GettableHashSet<EquivalenceClass> allPhrases = new GettableHashSet<EquivalenceClass>(phrases);
    int logCount = 0;

    while ((curLine = reader.readLine()) != null) {
      curLine = curLine.trim();
    
      // Split into likely sentences
      curSents = getSentences(curLine, corpusAccess.isOneSentencePerLine());
      
      // Within each sentence, get phrase position counts
      for (String sent : curSents) {
        
        sentPhrases = getAllIndexedPhrases(sent, allPhrases);
        
        for (IndexedPhrase idxPhrase : sentPhrases) {
          
          // Get/set its context phrase prop
          if ((phraseContext = (PhraseContext)idxPhrase.phrase.getProperty(PhraseContext.class.getName())) == null) {
            idxPhrase.phrase.setProperty(phraseContext = new PhraseContext());
          }
          
          for (IndexedPhrase contextIdxPhrase : sentPhrases) {
            if (idxPhrase.isAfter(contextIdxPhrase)) {
              phraseContext.addBefore(contextIdxPhrase.phrase);
              
              if (logCount > 0) {
                LOG.info("Phrase " + contextIdxPhrase.toString() + " precedes " + idxPhrase.toString() + " in sentence [" + sent + "]");
                logCount--;
              }
            }
            else if (idxPhrase.isBefore(contextIdxPhrase)) {
              phraseContext.addAfter(contextIdxPhrase.phrase);
              
              if (logCount > 0) {
                LOG.info("Phrase " + contextIdxPhrase.toString() + " follows " + idxPhrase.toString() + " in sentence [" + sent + "]");
                logCount--;
              }
            }
            // Anni's idea: only keep few near out of order phrases (closer than m_maxPhraseLength)
            else if (idxPhrase.isOutOfOrderButCloseEnough(contextIdxPhrase, sent, 1)) { //m_maxPhraseLength)) {
            //else if (idxPhrase.isOutOfOrder(contextIdxPhrase)) {
              phraseContext.addOutOfOrder(contextIdxPhrase.phrase);
              
              if (logCount > 0) {
                LOG.info("Phrase " + contextIdxPhrase.toString() + " is out of order with " + idxPhrase.toString() + " in sentence [" + sent + "]");
                logCount--;
              }
              
            }
          }
        }
      }
    }

    reader.close();
  }
  
  protected List<IndexedPhrase> getAllIndexedPhrases(String sent, GettableHashSet<EquivalenceClass> allPhrases) {
    
    // Get all sentence delimiters
    InvertibleHashMap<IdxPair, Integer> delimIdxs = getAllDelims(sent);    
    // Get all phrases up to length m_maxPhraseLength 
    List<IdxPair> sentPhraseIdxs = getAllPhraseIdxs(delimIdxs, m_maxPhraseLength);
    // Phrases containing index info along with indices of the preceding and following delimiters
    List<IndexedPhrase> idxPhrases = new LinkedList<IndexedPhrase>();

    HashMap<Integer, IdxPair> firstCharDelim = new HashMap<Integer, IdxPair>();
    HashMap<Integer, IdxPair> lastCharDelim = new HashMap<Integer, IdxPair>();
    IdxPair delimIdx;
    
    for (int j = 0; j < delimIdxs.size(); j++) {
      delimIdx = delimIdxs.getKey(j);
      firstCharDelim.put(delimIdx.from, delimIdx);
      lastCharDelim.put(delimIdx.to, delimIdx);
    }

    Phrase phrase;
    IdxPair phraseIdx;
    
    for (int i = 0; i < sentPhraseIdxs.size(); i++) {
     
      phraseIdx = sentPhraseIdxs.get(i);
      (phrase = new Phrase()).init(sent.substring(phraseIdx.from, phraseIdx.to), m_caseSensitive);
            
      // Only keep it if it is a phrase in our big list
      if (null != (phrase = (Phrase)allPhrases.get(phrase))) {
        idxPhrases.add(new IndexedPhrase(phrase, phraseIdx, delimIdxs.getValue(lastCharDelim.get(phraseIdx.from)), delimIdxs.getValue(firstCharDelim.get(phraseIdx.to))));
      }
    }
    
    return idxPhrases;
  }
  
  class IndexedPhrase {
    public IndexedPhrase(Phrase phrase, IdxPair idxPair, int ordDelimBefore, int ordDelimAfter) {
      this.idxPair = idxPair;
      this.phrase = phrase;
      this.ordDelimBefore = ordDelimBefore;
      this.ordDelimAfter = ordDelimAfter;
    }
    
    public boolean isBefore(IndexedPhrase other) {
      return (idxPair.to + 1) == other.idxPair.from;      
    }
    
    public boolean isAfter(IndexedPhrase other) {
      return (other.idxPair.to + 1) == idxPair.from;
    }
    
    // true iff the two phrases aren't next to each other and do not overlap
    public boolean isOutOfOrder(IndexedPhrase other) {
      return (other.idxPair.to + 1) < idxPair.from || (idxPair.to + 1) < other.idxPair.from;
    }
    
    // true iff the two phrases aren't next to each other and do not overlap and there are at most toksBetween tokens between them  
    public boolean isOutOfOrderButCloseEnough(IndexedPhrase other, String sent, int toksBetween) {
      
      int numToks = -1;

      if (other.ordDelimAfter <= ordDelimBefore) {
        numToks = ordDelimBefore - other.ordDelimAfter;
      } else if (ordDelimAfter <= other.ordDelimBefore) {
        numToks = other.ordDelimBefore - ordDelimAfter;
      }
      
      return numToks >= 0 && numToks <= toksBetween;
    }
    
    public String toString() {
      return "[" + phrase.toString() + "|" + idxPair.toString() + "]";
    }
    
    public Phrase phrase; // Phrase
    public IdxPair idxPair; // From/to indices in the sentence 
    public int ordDelimBefore; // ordinal number (in the sentence) of the preceding delimiter
    public int ordDelimAfter; // ordinal number (in the sentence) of the following delimiter
  }
}
