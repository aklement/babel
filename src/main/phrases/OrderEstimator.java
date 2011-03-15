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
import babel.content.eqclasses.phrases.PhraseTable.PairProps;
import babel.reordering.scorers.ReorderingScorer;
import babel.reordering.scorers.ReorderingScorer.OrderTriple;

public class OrderEstimator {

  protected static final Log LOG = LogFactory.getLog(OrderEstimator.class);
  protected static final int NUM_PAIRS_TO_GIVE = 10000;
  protected static final int PERCENT_REPORT = 5;
  
  public OrderEstimator(int numThreads, ReorderingScorer reordScorer, PhraseTable phraseTable, double maxPhrCountInTrg) {
    
    if (numThreads < 1) { 
      throw new IllegalArgumentException("Must request at least one thread");
    }
   
    m_numThreads = numThreads;
    m_reordScorer = reordScorer;
    m_maxPhrCountInTrg = maxPhrCountInTrg;
    m_phraseTable = phraseTable;
    m_workerIds = new ArrayList<Integer>(m_numThreads);
    m_phrasePairsToProcess = new LinkedList<PhrasePair>();
  }
  
  public synchronized void estimateReordering(Set<Phrase> srcPhrases) throws Exception {
    
    m_workerIds.clear();
    m_percentComplete = 0;
    m_completePairs = 0;
    m_percentThreshold = PERCENT_REPORT;

    m_phrasePairsToProcess.clear();
    
    // Collect all phrase pairs in phrase table with reordering property
    LOG.info(" - Selecting phrase pairs with reordering property ...");
    
    for (Phrase srcPhrase : srcPhrases) {
      for (Phrase trgPhrase : m_phraseTable.getTrgPhrases(srcPhrase)) {
        m_phrasePairsToProcess.add(new PhrasePair(srcPhrase, trgPhrase));
      }
    }
    
    m_totalPairs = m_phrasePairsToProcess.size();
    
    LOG.info(" - Estimating reordering for " + (int)m_totalPairs + " phrases with contextual phrases found in monolingual data ...");
    
    // Start up the worker threads
    for (int threadNum = 0; threadNum < m_numThreads; threadNum++) { 
      m_workerIds.add(threadNum);   
      (new Thread(new OrderWorker(this, threadNum))).start();
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

  protected void estimateReordering(Phrase srcPhrase, Phrase trgPhrase) {
     
    OrderTriple beforeFeats = m_reordScorer.scoreBefore(srcPhrase, trgPhrase);
    OrderTriple afterFeats = m_reordScorer.scoreAfter(srcPhrase, trgPhrase);
    PairProps props = m_phraseTable.getProps(srcPhrase, trgPhrase);
    
    if (beforeFeats != null) {
      props.setBeforeOrderFeatVals(beforeFeats.getMonoScore(), beforeFeats.getSwapScore(), beforeFeats.getDiscScore());
    }
      
    if (afterFeats != null) {
      props.setAfterOrderFeatVals(afterFeats.getMonoScore(), afterFeats.getSwapScore(), afterFeats.getDiscScore());
    }
  }

  protected int m_numThreads;
  protected ReorderingScorer m_reordScorer;
  protected PhraseTable m_phraseTable;
  protected List<Integer> m_workerIds;
  protected LinkedList<PhrasePair> m_phrasePairsToProcess;
  protected int m_percentComplete;
  protected int m_percentThreshold;
  protected double m_completePairs;
  protected double m_totalPairs;
  protected double m_maxPhrCountInTrg;
  
  class OrderWorker implements Runnable {
    
    public OrderWorker(OrderEstimator estimator, int workerId) {
      m_workerId = workerId;
      m_estimator = estimator;
    }
  
    public void run() {

      LOG.info(" - Worker " + m_workerId + " started estimating reordering features.");
    
      List<PhrasePair> phrasePairs;
      
      while (null != (phrasePairs = m_estimator.getPhrasePairsToProcess())) { 
        
        for (PhrasePair pair : phrasePairs) {
          m_estimator.estimateReordering(pair.srcPhrase(), pair.trgPhrase());
        }
        
        m_estimator.estimationDone(phrasePairs.size());
      }

      LOG.info(" - Worker " + m_workerId + " finished.");
    
      m_estimator.workerDone(m_workerId);
    }
  
    protected int m_workerId;
    protected OrderEstimator m_estimator;
  }
}
