package main.phrases;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhrasePair;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.phrases.PhraseTable.PairFeat;
import babel.content.eqclasses.phrases.PhraseTable.PairProps;
import babel.ranking.scorers.Scorer;
import babel.util.dict.SimpleDictionary;
import babel.util.misc.EditDistance;

public class AllPairsTokenFeatureEstimator {

  protected static final Log LOG = LogFactory.getLog(AllPairsTokenFeatureEstimator.class);
  protected static final int NUM_PAIRS_TO_GIVE = 1000;
  protected static final int PERCENT_REPORT = 5;

  protected AllPairsTokenFeatureEstimator(int numThreads, Scorer contextScorer, Scorer timeScorer, SimpleDictionary translitDict) {
    
    if (numThreads < 1) { 
      throw new IllegalArgumentException("Must request at least one thread");
    }
   
    m_numThreads = numThreads;
    m_contextScorer = contextScorer;
    m_timeScorer = timeScorer;
    m_translitDict = translitDict;
    m_workerIds = new ArrayList<Integer>(m_numThreads); 
    m_phrasePairsToProcess = new LinkedList<PhrasePair>();
  }  
  
  public AllPairsTokenFeatureEstimator(PhraseTable phraseTable, int numThreads, Scorer contextScorer, Scorer timeScorer, SimpleDictionary translitDict) {
    
    this(numThreads, contextScorer, timeScorer, translitDict);
    m_phraseTable = phraseTable;
  }
  
  public synchronized void estimateFeatures(Set<Phrase> srcPhrases) throws Exception {
    
    m_workerIds.clear();
    m_percentComplete = 0;
    m_completePairs = 0;
    m_percentThreshold = PERCENT_REPORT;
  
    m_phrasePairsToProcess.clear();
    
    for (Phrase srcPhrase : srcPhrases) {
      for (Phrase trgPhrase : m_phraseTable.getTrgPhrases(srcPhrase)) {         
        m_phrasePairsToProcess.add(new PhrasePair(srcPhrase, trgPhrase));
      }
    }
    
    m_totalPairs = m_phrasePairsToProcess.size();
        
    LOG.info(" - Estimating monolingual features for " + (int)m_totalPairs + " phrase pairs.");
    
    // Start up the worker threads
    for (int threadNum = 0; threadNum < m_numThreads; threadNum++) { 
      m_workerIds.add(threadNum);   
      (new Thread(new FeatureWorker(this, threadNum))).start();
    }
    
    // Wait until all threads are done
    while (m_workerIds.size() > 0) {
      wait();
    } 
  }
  
  protected synchronized List<PhrasePair> getPhrasePairsToProcess() {
    List<PhrasePair> pairsToProcess = null;

    // Give a worker thread a set of phrase pairs
    if (m_phrasePairsToProcess.size() > 0) {
      
      pairsToProcess = new ArrayList<PhrasePair>(NUM_PAIRS_TO_GIVE);
      
      for (int i = 0; (m_phrasePairsToProcess.size() > 0) && i < NUM_PAIRS_TO_GIVE; i++) {
        pairsToProcess.add(m_phrasePairsToProcess.remove());
      }
    }
    
    return pairsToProcess;
  }
    
  protected synchronized void estimationDone(int numComplete) {    
    
    m_completePairs += numComplete;

    if ((m_percentComplete = (int)(100 * m_completePairs / m_totalPairs)) >= m_percentThreshold) { 
      LOG.info(" - " + m_percentComplete + "% done.");
      m_percentThreshold += PERCENT_REPORT;      
    }
  }
  
  protected synchronized void workerDone(int workerID) {
    m_workerIds.remove(new Integer(workerID)); 
    notify();
  }

  protected void estimateFeatures(Phrase srcPhrase, Phrase trgPhrase) {
    PairProps props = m_phraseTable.getProps(srcPhrase, trgPhrase);

    double contextScore = m_contextScorer.score(srcPhrase, trgPhrase);
    double timeScore = m_timeScorer.score(srcPhrase, trgPhrase);
    
    props.setPairFeatVal(PairFeat.PH_CONTEXT, contextScore);
    props.setPairFeatVal(PairFeat.PH_TIME, timeScore);
    props.setPairFeatVal(PairFeat.LEX_CONTEXT, contextScore);
    props.setPairFeatVal(PairFeat.LEX_TIME, timeScore);  
    props.setPairFeatVal(PairFeat.LEX_EDIT, scoreEdit(srcPhrase, trgPhrase, props, m_translitDict));
  }
  
  // Compute average per character forward and backward edit distance
  protected double scoreEdit(Phrase srcPhrase, Phrase trgPhrase, PairProps props, SimpleDictionary translitDict) {
    assert srcPhrase.numTokens() == 1 && trgPhrase.numTokens() == 1;
    
    String srcWord = srcPhrase.getWord();
    String trgWord = trgPhrase.getWord();
    
    // Try transliterating source phrase
    if (translitDict != null) {
      srcWord = translitWord(srcWord, translitDict);
    }
      
    double numEdits = EditDistance.distance(srcWord, trgWord);
    double letterCount = (double)(srcWord.length() + trgWord.length()) / 2.0;
    
    return numEdits / letterCount;
  }
  
  protected String translitWord(String word, SimpleDictionary translitDict) {
    
    Set<String> translits = translitDict.getTrg(word);  
    return (null != translits) ? translits.iterator().next() : word;
  }

  protected PhraseTable m_phraseTable;
  protected Scorer m_contextScorer;
  protected Scorer m_timeScorer;
  protected SimpleDictionary m_translitDict;
  protected int m_numThreads;
  protected List<Integer> m_workerIds;
  protected LinkedList<PhrasePair> m_phrasePairsToProcess;
  protected int m_percentComplete;
  protected int m_percentThreshold;
  protected double m_completePairs;
  protected double m_totalPairs;
  
  class FeatureWorker implements Runnable {
    
    public FeatureWorker(AllPairsTokenFeatureEstimator estimator, int workerId) {
      m_workerId = workerId;
      m_estimator = estimator;
    }
  
    public void run() {

      LOG.info(" - Worker " + m_workerId + " started estimating monolingual features.");
    
      List<PhrasePair> phrasePairs;
      
      while (null != (phrasePairs = m_estimator.getPhrasePairsToProcess())) { 
        
        for (PhrasePair pair : phrasePairs) {
          m_estimator.estimateFeatures(pair.srcPhrase(), pair.trgPhrase());
        }
        
        m_estimator.estimationDone(phrasePairs.size());
      }

      LOG.info(" - Worker " + m_workerId + " finished.");
    
      m_estimator.workerDone(m_workerId);
    }
  
    protected int m_workerId;
    protected AllPairsTokenFeatureEstimator m_estimator;
  }
}
