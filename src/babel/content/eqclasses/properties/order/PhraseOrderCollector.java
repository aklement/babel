package babel.content.eqclasses.properties.order;

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
import babel.content.eqclasses.properties.PhrasePropertyCollector;
import babel.util.misc.GettableHashSet;
import babel.util.misc.InvertibleHashMap;

public class PhraseOrderCollector extends PhrasePropertyCollector {

  public static final Log LOG = LogFactory.getLog(PhrasePropertyCollector.class);
  
  public PhraseOrderCollector(boolean src, int maxPhraseLength, int maxToksBetween, boolean collectLongestOnly, boolean caseSensitive, long maxPhrCount, Set<? extends EquivalenceClass> allPhrases, double keepContPhraseProb) {    
    super(maxPhraseLength, caseSensitive);
    
    m_src = src;
    m_maxPhrCount = maxPhrCount;
    m_maxToksBetween = maxToksBetween;
    m_collectLongestOnly = collectLongestOnly;
    m_allPhrases = new GettableHashSet<EquivalenceClass>(allPhrases);
    m_keepContPhraseProb = keepContPhraseProb;
  }

  public void collectProperty(CorpusAccessor corpusAccess, Set<? extends EquivalenceClass> phrases) throws Exception {
    
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    List<String> curSents;
    PhraseContext phraseContext;
    //Number phraseCount;
    List<IndexedPhrase> sentPhrases;
    int logCount = 0;
    int sentCount = 0;
    
    IndexedPhrase beforePhrase, afterPhrase, discontPhrase;

    //Set<IndexedPhrase> discontPhrases = new HashSet<IndexedPhrase>();

    long beforeCount = 0, afterCount = 0, discCount = 0, allDiscCount = 0;
    
    
    
    while ((curLine = reader.readLine()) != null) {
      curLine = curLine.trim();
    
      // Split into likely sentences
      curSents = getSentences(curLine, corpusAccess.isOneSentencePerLine());
      
      // Within each sentence, get phrase position counts
      for (String sent : curSents) {
        
        if (((sentCount++ % 100000) == 0) && (sentCount != 1)) {
          LOG.info((sentCount-1) + (m_src ? " source" : " target") + " sents processed for reordering.");
        }
        
        sentPhrases = getAllIndexedPhrases(sent, m_allPhrases);
        
        for (IndexedPhrase idxPhrase : sentPhrases) {
          
          
          
          
          
          // TODO: Throw away most frequent phrases (occurring more often than 70% as often as the most frequent phrase)
          //if (((phraseCount = (Number)idxPhrase.phrase.getProperty(Number.class.getName())) != null) && (phraseCount.getNumber() > 0.7 * m_maxPhrCount)) {            
          //  break;
          //}   
          
          
          
          
          // Get/set its context phrase prop
          if ((phraseContext = (PhraseContext)idxPhrase.phrase.getProperty(PhraseContext.class.getName())) == null) {
            idxPhrase.phrase.setProperty(phraseContext = new PhraseContext(m_keepContPhraseProb));
          }
          
                
          // TODO: Alternative idea -> only keep the longest contextual phrase
          beforePhrase = afterPhrase = discontPhrase = null;
          //discontPhrases.clear();

          for (IndexedPhrase contextIdxPhrase : sentPhrases) {
            if (idxPhrase.isAfter(contextIdxPhrase)) {
              
              if ((beforePhrase == null) || !m_collectLongestOnly || (beforePhrase.phrase.numTokens() < contextIdxPhrase.phrase.numTokens())) {
                beforePhrase = contextIdxPhrase;
              }
            }
            else if (idxPhrase.isBefore(contextIdxPhrase)) {
              
              if ((afterPhrase == null) || !m_collectLongestOnly || (afterPhrase.phrase.numTokens() < contextIdxPhrase.phrase.numTokens())) {
                afterPhrase = contextIdxPhrase;
              }              
            }
            else if (!m_src && idxPhrase.isOutOfOrderButCloseEnough(contextIdxPhrase, m_maxToksBetween)) {
              
              if ((discontPhrase == null) || !m_collectLongestOnly || (discontPhrase.phrase.numTokens() < contextIdxPhrase.phrase.numTokens())) {
                discontPhrase = contextIdxPhrase;
              }
            }
            
            //else if (!m_src && idxPhrase.isOutOfOrder(contextIdxPhrase)) {
            //  
            //  if ((discontPhrase == null) || (discontPhrase.phrase.numTokens() < contextIdxPhrase.phrase.numTokens())) {
            //    discontPhrase = contextIdxPhrase;
            //  }
            //}
            
            /*
            else if (!m_src && idxPhrase.isOutOfOrderButCloseEnough(contextIdxPhrase, sent, m_maxPhraseLength)) {

              discontPhrases.add(contextIdxPhrase);
            }*/
            

            
            /*
            else if (!m_src && idxPhrase.isOutOfOrderButCloseEnough(contextIdxPhrase, sent, m_maxPhraseLength)) {
              
              phraseContext.addOutOfOrder(contextIdxPhrase.phrase);
              
              if (logCount > 0) {
                LOG.info("Phrase " + contextIdxPhrase.phrase.toString() + " is discontinuous with " + idxPhrase.toString() + " in sentence [" + sent + "]");
                logCount--;
              }
              
            }*/
          }

          if (beforePhrase != null) {
            phraseContext.addBefore(beforePhrase.phrase);
            
            if (logCount > 0) {
              LOG.info("Phrase " + beforePhrase.toString() + " precedes " + idxPhrase.toString() + " in sentence [" + sent + "]");
              logCount--;
            }
            
            beforeCount++;
          }
          if (afterPhrase != null) {
            phraseContext.addAfter(afterPhrase.phrase);
            
            if (logCount > 0) {
              LOG.info("Phrase " + afterPhrase.toString() + " follows " + idxPhrase.toString() + " in sentence [" + sent + "]");
              logCount--;
            }
            
            afterCount++;
          }
          /*
          if (discontPhrases.size() > 0) {
            
            boolean contained;
            
            for (IndexedPhrase discPhrase : discontPhrases) {
              
              contained = false;
              
              for (IndexedPhrase other : discontPhrases) {            
                
                // If the phrase is contained in some other phrase give up - do not add it
                if (discPhrase != other && other.contains(discPhrase)) {
                  contained = true;
                  break;
                }
                
              }
                            
              if (!contained) {
                phraseContext.addOutOfOrder(discPhrase.phrase);
             
                if (logCount > 0) {
                  LOG.info("Phrase " + discPhrase.toString() + " is discontinous with " + idxPhrase.toString() + " in sentence [" + sent + "]");
                  logCount--;
                }
                
                discCount++;
              }
              
              allDiscCount++;
            }
          }*/
                    
          
          if (discontPhrase != null) {
            phraseContext.addOutOfOrder(discontPhrase.phrase);
            
            if (logCount > 0) {
              LOG.info("Phrase " + discontPhrase.toString() + " is discontinuous with " + idxPhrase.toString() + " in sentence [" + sent + "]");
              logCount--;
            }
            
            discCount++;
            allDiscCount++;
          }

 
          /*
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
          */
          
        }
      }
    }
    
    LOG.info("Total collected for " + (m_src ? "source" : "target") + ": before = " + beforeCount + ", after = " + afterCount+ " and discontinuouos = " + discCount + " (out of " + allDiscCount + ")");

    reader.close();
  }
  
  protected List<IndexedPhrase> getAllIndexedPhrases(String sent, GettableHashSet<EquivalenceClass> allPhrases) {
    
    // Get all sentence delimiters
    InvertibleHashMap<IdxPair, Integer> delimIdxs = getAllDelims(sent, PHRASE_DELIMS);    
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
  
  protected boolean m_src;
  protected long m_maxPhrCount;
  protected GettableHashSet<EquivalenceClass> m_allPhrases;
  protected double m_keepContPhraseProb;
  protected int m_maxToksBetween;
  protected boolean m_collectLongestOnly;
  
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
    
    // true iff other is within this phrase 
    public boolean contains(IndexedPhrase other) {
      return (idxPair.from <= other.idxPair.from) && (idxPair.to >= other.idxPair.to);
    }
    
    public boolean overlaps(IndexedPhrase other) {
      boolean noOverlap = (other.idxPair.to <= idxPair.from || other.idxPair.from >= idxPair.to);
      return !noOverlap;
    }
    
    // true iff the two phrases aren't next to each other and do not overlap and there are at most toksBetween tokens between them  
    public boolean isOutOfOrderButCloseEnough(IndexedPhrase other, int toksBetween) {
      
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
