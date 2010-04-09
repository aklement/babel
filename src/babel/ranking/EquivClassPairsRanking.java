package babel.ranking;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.ranking.EquivClassCandRanking.ScoredCandidate;

public class EquivClassPairsRanking
{
  public EquivClassPairsRanking(boolean smallerIsBetter, int numBest)
  {
    if (numBest <= 0)
    { throw new IllegalArgumentException("Pair list size must be positive");
    }
    
    m_numBest = numBest;
    m_smallerIsBetter = smallerIsBetter;
    m_pairs = new LinkedList<ScoredPair>();
  }

  public EquivClassPairsRanking(boolean smallerIsBetter, int numBest, Collection<EquivClassCandRanking> rankings, int maxNumTrgPerSource)
  {
    this(smallerIsBetter, numBest);
    addPairsFromRankingCollection(rankings, maxNumTrgPerSource);
  }

  public void threshold(double lower, double upper)
  {
    double score;
    
    for (int idx = (m_pairs.size() - 1); idx >= 0; idx--)
    {
      score = m_pairs.get(idx).m_score;

      if (score < lower || score > upper)
      { m_pairs.remove(idx);
      }
    }
  }
  
  public void addPairsFromRankingCollection(Collection<EquivClassCandRanking> rankings, int maxNumTrgPerSource)
  {
    for (EquivClassCandRanking ranking : rankings)
    {
      addPairsFromRanking(ranking, maxNumTrgPerSource);
    }
  }
  
  /**
   * Adds (up to maxPairs, or all if maxPairs < 0) pairs from the given 
   * rankinging.
   */
  public void addPairsFromRanking(EquivClassCandRanking ranking, int maxPairs)
  {
    EquivalenceClass srcEq = ranking.getEqClass();
    List<ScoredCandidate> candidates = ranking.getScoredCandidates();
    int numToCheck = candidates.size();
    numToCheck = ((maxPairs < 0) || (maxPairs >= numToCheck)) ? numToCheck : maxPairs;
    ScoredCandidate scoredTrgEq;
    
    for (int num = 0; num < numToCheck; num++)
    {
      scoredTrgEq = candidates.get(num);
      
      if (!add(srcEq, scoredTrgEq.getCandidate(), scoredTrgEq.getScore()))
      {
        // If couldn't add this one, no sense to continue trying to add worse candidates
        break;
      }
    }
  }
  
  public boolean add(EquivalenceClass srcEq, EquivalenceClass trgEq, double score)
  {
    ScoredPair newPair = new ScoredPair(score, srcEq, trgEq);
    int insIdx = Collections.binarySearch(m_pairs, newPair);
    int listSize = m_pairs.size();
    boolean added = false;
    
    if (insIdx < 0)
    { insIdx = -(insIdx + 1);
    }
    
    // If there is room or it's better than the current worst scoring element - insert
    if (m_numBest < 0 || listSize < m_numBest || insIdx < listSize)
    {
      m_pairs.add(insIdx, newPair);
      added = true;
      
      // Trim the size if too large - drop the last (worst) element.
      if (listSize == m_numBest)
      { m_pairs.remove(listSize);
      }
    }
    
    return added;
  }
  
  public List<ScoredPair> getScoredPairs()
  {
    return m_pairs;
  }
  
  public void clear()
  {
    m_pairs.clear();
  }
  
  public String toString()
  {
    StringBuffer strBuf = new StringBuffer();       

    for (ScoredPair pair : m_pairs)
    {
      strBuf.append(pair.toString());
      strBuf.append("\n");
    }
    
    return strBuf.toString();
  }
  
  public void dumpToFile(String fileName) throws Exception
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for(ScoredPair pair : m_pairs)
    {
      writer.write(pair.toString());
      writer.newLine();
    }
    
    writer.close();
  }
  
  protected LinkedList<ScoredPair> m_pairs;
  protected boolean m_smallerIsBetter;
  protected int m_numBest;
  
  /**
   * Manages a candidate / score pair.
   */
  public class ScoredPair implements Comparable<ScoredPair>
  {
    public ScoredPair(double score, EquivalenceClass srcEq, EquivalenceClass trgEq)
    {
      if ((srcEq == null) || (trgEq == null))
      { throw new IllegalArgumentException("No source or target EquivalenceClass specified.");
      }
      
      m_srcEq = srcEq;
      m_trgEq = trgEq;
      m_score = score;
    }
    
    public EquivalenceClass getSrcEq()
    {
      return m_srcEq;
    }
    
    public EquivalenceClass getTrgEq()
    {
      return m_trgEq;
    }
    
    public double getScore()
    {
      return m_score;
    }
    
    public String toString()
    {
      return m_srcEq + "\t" + m_trgEq + "\t" + m_score;
    }
 
    /**
     * Used to sort the candidates in descending order (best scoring candidates
     * first).
     */
    public int compareTo(ScoredPair otherCand)
    {
      int score = 0;
      
      if (m_score > otherCand.m_score)
      { score = -1;
      }
      else if (m_score < otherCand.m_score)
      { score = 1;
      }
      
      return (m_smallerIsBetter ? -score : score);
    }

    protected EquivalenceClass m_srcEq;
    protected EquivalenceClass m_trgEq;
    protected double m_score;
  }
}
