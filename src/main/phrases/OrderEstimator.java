package main.phrases;

import java.util.ArrayList;
//import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhrasePair;
import babel.content.eqclasses.phrases.PhraseTable;
//import babel.content.eqclasses.phrases.PhraseTable.PairFeat;
import babel.content.eqclasses.phrases.PhraseTable.PairProps;
import babel.content.eqclasses.properties.PhraseContext;

public class OrderEstimator {

  protected static final Log LOG = LogFactory.getLog(OrderEstimator.class);
  protected static final int NUM_PAIRS_TO_GIVE = 10000;
  protected static final int PERCENT_REPORT = 5;
  
  public OrderEstimator(PhraseTable phraseTable, int numThreads, double maxPhrCountInTrg) {
    
    if (numThreads < 1) { 
      throw new IllegalArgumentException("Must request at least one thread");
    }
   
    m_maxPhrCountInTrg = maxPhrCountInTrg;
    m_phraseTable = phraseTable;
    m_numThreads = numThreads;
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
//    PhraseContext srcCont, trgCont;
//    HashSet<Phrase> srcContAll = new HashSet<Phrase>();
//    HashSet<Phrase> trgContAll = new HashSet<Phrase>();
    
    LOG.info(" - Selecting phrase pairs with reordering property ...");
    
    for (Phrase srcPhrase : srcPhrases) {
      if (null != (/*srcCont = */(PhraseContext)srcPhrase.getProperty(PhraseContext.class.getName()))) {
        for (Phrase trgPhrase : m_phraseTable.getTrgPhrases(srcPhrase)) {
          if (null != (/*trgCont = */(PhraseContext)trgPhrase.getProperty(PhraseContext.class.getName()))) {
            m_phrasePairsToProcess.add(new PhrasePair(srcPhrase, trgPhrase));
//            srcContAll.addAll(srcCont.getAll());
//            trgContAll.addAll(trgCont.getAll());
          }
        }
      }
    }

//    LOG.info(" - Smaller phrasetable contains " + srcContAll.size() + " source and " + trgContAll.size() + " target phrases.");
    
    m_totalPairs = m_phrasePairsToProcess.size();
    // Smaller effective phrase table => quicker reordering score estimations
    m_smallPhraseTable = m_phraseTable;//new PhraseTable(m_phraseTable, srcContAll, trgContAll);    
    
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
     
    PhraseContext srcPhraseContext = (PhraseContext)srcPhrase.getProperty(PhraseContext.class.getName());
    PhraseContext trgPhraseContext = (PhraseContext)trgPhrase.getProperty(PhraseContext.class.getName());

    double weight;
    int logCount = 0;
    double numMono, numSwap, numDiscont;
    
    Map<Phrase, Integer> beforeSrcPhrases = srcPhraseContext.getBefore();        
    double numBeforeMono = 0, numBeforeSwap = 0, numBeforeOutOfOrder = 0;

    int count = 0;
    
    for (Phrase beforeSrcPhrase : beforeSrcPhrases.keySet()) {
      
      if (count++ >= 1000) {
        break;
      }
      
      for (Phrase transTrgPhrase : m_smallPhraseTable.getTrgPhrases(beforeSrcPhrase)) {
        //count++;
        if (trgPhraseContext.hasAnywhere(transTrgPhrase)) {
          
          weight = 1.0;
          //weight = m_phraseTable.getProps(beforeSrcPhrase, transTrgPhrase).getPairFeatVal(PairFeat.EF); // PairFeat.EF  
                
          numBeforeMono += weight * (numMono = trgPhraseContext.beforeCount(transTrgPhrase));
          numBeforeSwap += weight * (numSwap = trgPhraseContext.afterCount(transTrgPhrase));
          numBeforeOutOfOrder += weight * (numDiscont = trgPhraseContext.outOfOrderCount(transTrgPhrase));
        
          if (logCount > 0) {
            
            StringBuilder strBldLog = new StringBuilder();
                  
            if (numMono > 0) {
              strBldLog.append("Mono (" +  numMono + ") : ");
            } else if (numSwap > 0) {
              strBldLog.append("Swap (" +  numSwap + ") : ");
            } else {
              strBldLog.append("Discontinuous (" +  numDiscont + ") : ");
            }
                  
            strBldLog.append(" phrase pair: (" + srcPhrase.toString() + "|" + trgPhrase.toString() + ")");
            strBldLog.append(", context phrase translations: (" + beforeSrcPhrase.toString() + "->" + transTrgPhrase.toString() + ")");
            strBldLog.append(", phrase table weight: " + weight);
            LOG.info(strBldLog.toString());
                    
            logCount--;                  
          }
        }
      }
    }

    Map<Phrase, Integer> afterSrcPhrases = srcPhraseContext.getAfter();
    double numAfterMono = 0, numAfterSwap = 0, numAfterOutOfOrder = 0;
    count = 0;
    
    for (Phrase afterSrcPhrase : afterSrcPhrases.keySet()) {

      if (count++ >= 1000) {
        break;
      }
      
      for (Phrase transTrgPhrase : m_smallPhraseTable.getTrgPhrases(afterSrcPhrase)) {
        if (trgPhraseContext.hasAnywhere(transTrgPhrase)) {

          weight = 1.0;
          //weight = m_phraseTable.getProps(afterSrcPhrase, transTrgPhrase).getPairFeatVal(PairFeat.EF); // PairFeat.EF
                
          numAfterMono += weight * (numMono = trgPhraseContext.afterCount(transTrgPhrase));
          numAfterSwap += weight * (numSwap = trgPhraseContext.beforeCount(transTrgPhrase));
          numAfterOutOfOrder += weight * (numDiscont = trgPhraseContext.outOfOrderCount(transTrgPhrase));

          if (logCount > 0) {
                  
            StringBuilder strBldLog = new StringBuilder();
                  
            if (numMono > 0) {
              strBldLog.append("Mono (" +  numMono + ") : ");
            } else if (numSwap > 0) {
              strBldLog.append("Swap (" +  numSwap + ") : ");
            } else {
              strBldLog.append("Discontinuous (" +  numDiscont + ") : ");
            }
                  
            strBldLog.append(" phrase pair: (" + srcPhrase.toString() + "|" + trgPhrase.toString() + ")");
            strBldLog.append(", context phrase translations: (" + afterSrcPhrase.toString() + "->" + transTrgPhrase.toString() + ")");
            strBldLog.append(", phrase table weight: " + weight);
            LOG.info(strBldLog.toString());
                    
            logCount--;
          }                  
        }
      }
    }

    // Write out the features
    double totalBefore = numBeforeMono + numBeforeSwap + numBeforeOutOfOrder;
    double totalAfter = numAfterMono + numAfterSwap + numAfterOutOfOrder;
    
    if ((totalBefore != 0) || (totalAfter != 0)) {
      PairProps props = m_phraseTable.getProps(srcPhrase, trgPhrase);

      if ((totalBefore != 0)) {
        props.setBeforeOrderFeatVals(numBeforeMono / totalBefore, numBeforeSwap / totalBefore, numBeforeOutOfOrder / totalBefore);
      }
      
      if (totalAfter != 0) {
        props.setAfterOrderFeatVals(numAfterMono / totalAfter, numAfterSwap / totalAfter, numAfterOutOfOrder / totalAfter);
      }
    }
    
    //LOG.info("Pair <" + srcPhrase.toString() + "|" + trgPhrase.toString() + "> needed " + count + " comparisons.");
  }

  protected PhraseTable m_phraseTable;
  protected PhraseTable m_smallPhraseTable;
  protected int m_numThreads;
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
