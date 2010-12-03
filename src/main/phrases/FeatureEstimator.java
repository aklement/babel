package main.phrases;

import java.util.ArrayList;
import java.util.HashMap;
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

public class FeatureEstimator {

  protected static final Log LOG = LogFactory.getLog(FeatureEstimator.class);
  protected static final int NUM_PAIRS_TO_GIVE = 1000;
  protected static final int PERCENT_REPORT = 5;
  
  public FeatureEstimator(PhraseTable phraseTable, int numThreads, Scorer contextScorer, Scorer timeScorer, SimpleDictionary translitDict) {
    
    if (numThreads < 1) { 
      throw new IllegalArgumentException("Must request at least one thread");
    }
   
    m_phraseTable = phraseTable;
    m_numThreads = numThreads;
    m_contextScorer = contextScorer;
    m_timeScorer = timeScorer;
    m_translitDict = translitDict;
    m_workerIds = new ArrayList<Integer>(m_numThreads); 
    m_phrasePairsToProcess = new LinkedList<PhrasePair>();
    m_srcToks = new HashMap<String, Phrase>();
    m_trgToks = new HashMap<String, Phrase>();
    
    for (Phrase srcPhrase : phraseTable.getAllSrcPhrases()) {
      
      if (srcPhrase.numTokens() == 1) {
        m_srcToks.put(srcPhrase.toString(), srcPhrase);
      }
      
      for (Phrase trgPhrase : phraseTable.getTrgPhrases(srcPhrase)) {         
        m_phrasePairsToProcess.add(new PhrasePair(srcPhrase, trgPhrase));
        
        if (trgPhrase.numTokens() == 1) {
          m_trgToks.put(trgPhrase.toString(), trgPhrase);
        }
      }
    }
       
    m_totalPairs = m_phrasePairsToProcess.size();
  }
  
  public synchronized void estimateFeatures() throws Exception {
    
    m_workerIds.clear();
    m_percentComplete = 0;
    m_completePairs = 0;
    m_percentThreshold = PERCENT_REPORT;
    
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
    
    double[] scores = scoreAverage(srcPhrase, trgPhrase, props, new Scorer[]{m_contextScorer, m_timeScorer});
    props.setPairFeatVal(PairFeat.CONTEXT, scores[0]);
    props.setPairFeatVal(PairFeat.TIME, scores[1]);  
    
    //props.setPairFeatVal(PairFeat.CONTEXT, m_contextScorer.score(srcPhrase, trgPhrase));
    //props.setPairFeatVal(PairFeat.TIME, m_timeScorer.score(srcPhrase, trgPhrase));
    props.setPairFeatVal(PairFeat.EDIT, scoreEdit(srcPhrase, trgPhrase, props, m_translitDict));
  }
  
  // Compute average per character forward and backward edit distance
  protected double scoreEdit(Phrase srcPhrase, Phrase trgPhrase, PairProps props, SimpleDictionary translitDict) {
    String[] srcWords = srcPhrase.getWord().split(" ");
    String[] trgWords = trgPhrase.getWord().split(" ");
    
    // Try transliterating source phrase
    if (translitDict != null) {
      translitWords(srcWords, translitDict);
    }
    
    double letterCount = 0;
    double numEdits = 0; 
    
    // Forward counts
    int[][] aligns = props.getForwardAligns();
    for (int i = 0; i < aligns.length; i++) {
      if (aligns[i] != null) {
        for (int j = 0; j < aligns[i].length; j++) {
          numEdits += EditDistance.distance(srcWords[i], trgWords[aligns[i][j]]);
          letterCount += (double)(srcWords[i].length() + trgWords[aligns[i][j]].length()) / 2.0;
        }
      }
    }
    
    // Backward counts
    aligns = props.getBackwardAligns();
    for (int i = 0; i < aligns.length; i++) {
      if (aligns[i] != null) {
        for (int j = 0; j < aligns[i].length; j++) {
          numEdits += EditDistance.distance(trgWords[i], srcWords[aligns[i][j]]);
          letterCount += (double)(trgWords[i].length() + srcWords[aligns[i][j]].length()) / 2.0;
        }
      }
    }
    
    return numEdits / letterCount;
  }
  
  protected void translitWords(String[] words, SimpleDictionary translitDict) {
    
    Set<String> translits;
    
    for (int i = 0; i < words.length; i++) {
      if (null != (translits = translitDict.getTrg(words[i]))) {
        words[i] = translits.iterator().next();
      }
    }
  }
  
  protected double[] scoreAverage(Phrase srcPhrase, Phrase trgPhrase, PairProps props, Scorer[] scorers) {
  
    String[] srcWords = srcPhrase.getWord().split(" ");
    String[] trgWords = trgPhrase.getWord().split(" ");
    
    // TODO: Inefficient
    double numAligns = 0;
    double[] scores = new double[scorers.length];
    Phrase srcTok, trgTok;
    
    double alignedToks = 0;
    
    // Forward counts
    int[][] aligns = props.getForwardAligns();
    for (int i = 0; i < aligns.length; i++) {
      
      if (aligns[i] != null) {
        alignedToks++;
      }
      
      srcTok = m_srcToks.get(srcWords[i]);
      
      if (srcTok != null && aligns[i] != null) {
        for (int j = 0; j < aligns[i].length; j++) {
          
          if (null != (trgTok = m_trgToks.get(trgWords[aligns[i][j]]))) {
         
            for (int s = 0; s < scorers.length; s++) {
              scores[s] += scorers[s].score(srcTok, trgTok);
            }
          
            numAligns++;
          }
        }
      }
    }
    
    // Backward counts
    aligns = props.getBackwardAligns();
    for (int i = 0; i < aligns.length; i++) {

      if (aligns[i] != null) {
        alignedToks++;
      }
      
      trgTok = m_trgToks.get(trgWords[i]);
      
      if (trgTok != null && aligns[i] != null) {
        for (int j = 0; j < aligns[i].length; j++) {
          
          if (null != (srcTok = m_srcToks.get(srcWords[aligns[i][j]]))) {
          
            for (int s = 0; s < scorers.length; s++) {
              scores[s] += scorers[s].score(trgTok, srcTok);
            }
          
            numAligns++;
          }
        }
      }
    }
    
    double penalty = alignedToks / (double)(srcWords.length + trgWords.length);
    
    for (int s = 0; s < scorers.length; s++) {
      scores[s] /= numAligns;
      scores[s] *= penalty;
    }
    
    return scores;
  }

  protected PhraseTable m_phraseTable;
  /** Single token source phrases from the phrase table. */
  protected HashMap<String, Phrase> m_srcToks;
  /** Single token target phrases from the phrase table. */
  protected HashMap<String, Phrase> m_trgToks;
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
    
    public FeatureWorker(FeatureEstimator estimator, int workerId) {
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
    protected FeatureEstimator m_estimator;
  }
}
