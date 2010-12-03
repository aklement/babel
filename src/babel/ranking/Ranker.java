package babel.ranking;

import java.util.Collection;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.ranking.scorers.Scorer;

import babel.content.eqclasses.EquivalenceClass;

public class Ranker
{
  public static final Log LOG = LogFactory.getLog(Ranker.class);
    
  /**
   * @param numBest number of best candidates to collect for each EquivalenceClass
   */
  public Ranker(Scorer scorer, int numBest, double threshold, int numThreads)
  {
    if (numThreads < 1)
    { throw new IllegalArgumentException("Must request at least one thread");
    }
   
    m_numThreads = numThreads;
    m_useThreshold = true;
    m_threshold = threshold;
    m_numBest = numBest;
    m_scorer = scorer;
    m_workerIds = new ArrayList<Integer>(m_numThreads); 
    m_curBestMatches = null;
  }
  
  /**
   * @param numBest number of best candidates to collect for each EquivalenceClass
   */
  public Ranker(Scorer scorer, int numBest, int numThreads)
  {
    this(scorer, numBest, 0, numThreads);
    m_useThreshold = false;
  }
  
  public String getId()
  {
    return m_scorer.getClass().getName();
  }
  
  /**
   * @return EquivClassRanking for the source EquivalenceClass object from the
   *   target EquivalenceClass collection.
   */
  public EquivClassCandRanking getBestCandList(EquivalenceClass srcEq, Collection<EquivalenceClass> trgEqs)
  {
    boolean smallerIsBetter = m_scorer.smallerScoresAreBetter();
    EquivClassCandRanking candSet = new EquivClassCandRanking(srcEq, m_numBest, smallerIsBetter);
    double score;
  
    for (EquivalenceClass trgEq : trgEqs)
    {
      score = m_scorer.score(srcEq, trgEq);
      
      // If using threshold - only add candididates scoring no worse than it
      if (!m_useThreshold ||
          (m_useThreshold && ((smallerIsBetter && (score < m_threshold)) || 
                             (!smallerIsBetter && (score > m_threshold)))))
      {
        candSet.add(trgEq, score);
      }
    }
    
    return candSet;
  }

  /**
   * @return collection of EquivClassRanking objects (of size <= numBest)
   *   from trgEqs, one for each source EquivalenceClass.
   * @throws Exception 
   */
  public synchronized Collection<EquivClassCandRanking> getBestCandLists(Collection<EquivalenceClass> srcEqs, Collection<EquivalenceClass> trgEqs) throws Exception
  {
    m_curBestMatches = new ArrayList<EquivClassCandRanking>(srcEqs.size());
    m_curSrcEqsToProcess = new LinkedList<EquivalenceClass>(srcEqs); 
    m_workerIds.clear();
    m_percentComplete = 0;
    m_srcCount = srcEqs.size();
       
    // Start up the worker threads
    for (int threadNum = 0; threadNum < m_numThreads; threadNum++)
    { 
      m_workerIds.add(threadNum);   
      (new Thread(new RankerWorker(this, threadNum, trgEqs))).start();
    }
    
    // Wait until all threads are done
    while (m_workerIds.size() > 0)
    { wait();
    }
 
    return m_curBestMatches;
  }
  
  protected synchronized EquivalenceClass getSrcEqToProcess()
  {
    // Give a worker thread one src EquivalenceClass at a time
    return (m_curSrcEqsToProcess.size() != 0) ? m_curSrcEqsToProcess.remove() : null;
  }
    
  protected synchronized void rankingDone(EquivClassCandRanking ranking)
  {
    m_curBestMatches.add(ranking);   
    
    int newPercent;
    
    if (((newPercent = (int)(100 * m_curBestMatches.size() / m_srcCount)) % 5 == 0) && (newPercent != m_percentComplete) && (LOG.isInfoEnabled())) 
    { 
      m_percentComplete = newPercent;
      
      //System.out.println(m_percentComplete + "% done.");
      LOG.info(m_percentComplete + "% done.");
    }
  }
  
  protected synchronized void workerDone(int workerID)
  {
    m_workerIds.remove(new Integer(workerID)); 
    notify();
  }
  
  /** Number of best candidates to keep for each EquivalenceClass. */
  protected int m_numBest;
  /** Threshold that has to be passed to be considered for the candidate list. */
  protected double m_threshold;
  /** Whether or not to use the threshold. */
  protected boolean m_useThreshold;
  /** Metric to be used when collecting candidates. */
  protected Scorer m_scorer;
  protected int m_numThreads;
  protected List<Integer> m_workerIds;
  ArrayList<EquivClassCandRanking> m_curBestMatches;
  LinkedList<EquivalenceClass> m_curSrcEqsToProcess;
  int m_srcCount;
  int m_percentComplete;

  class RankerWorker implements Runnable
  {
    public RankerWorker(Ranker ranker, int workerId, Collection<EquivalenceClass> trgEqs)
    {
      m_workerId = workerId;
      m_ranker = ranker;
      m_trgEqs = trgEqs;
    }
  
    public void run()
    {
      if (Ranker.LOG.isInfoEnabled())
      { Ranker.LOG.info("Worker " + m_workerId + " started collecting rankings.");
      }    
    
      EquivalenceClass srcEq;

      while (null != (srcEq = m_ranker.getSrcEqToProcess()))
      { m_ranker.rankingDone(m_ranker.getBestCandList(srcEq, m_trgEqs)); // TODO: Make sure thread safe
      }

      if (Ranker.LOG.isInfoEnabled())
      { Ranker.LOG.info("Worker " + m_workerId + " finished.");
      }
    
      m_ranker.workerDone(m_workerId);
    }
  
    protected Collection<EquivalenceClass> m_trgEqs;
    protected int m_workerId;
    protected Ranker m_ranker;
  }
}