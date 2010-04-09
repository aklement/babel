package babel.ranking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;

/**
 * Simple container prividing a mechanism for associating a list of scored
 * candidates with a source EquivalenceClass object.
 */
public class EquivClassCandRanking
{
  protected static final Log LOG = LogFactory.getLog(EquivClassCandRanking.class);

  /**
   * @param EquivalenceClass a EquivalenceClass object with which this list is associated
   * @param numBest maximum number of candidates to keep in the list (neg valuw means all of them)
   * @param smallerIsBetter the smaller the score the better
   */
  public EquivClassCandRanking(EquivalenceClass eqClass, int numBest, boolean smallerIsBetter)
  {
    if (eqClass == null)
    { throw new IllegalArgumentException("EquivalenceClass is null.");
    }
    
    m_eq = eqClass;
    m_numBest = numBest;
    m_smallerIsBetter = smallerIsBetter;
    m_scoredCands = (numBest < 0) ? new LinkedList<ScoredCandidate>() : new ArrayList<ScoredCandidate>(numBest + 1);
    m_cands = new HashMap<EquivalenceClass, Double>();
    m_rand = new Random(1);
  }
  
  @SuppressWarnings("unchecked")
  public EquivClassCandRanking clone()
  {
    EquivClassCandRanking other = new EquivClassCandRanking(m_eq, m_numBest, m_smallerIsBetter);
    other.m_cands = (HashMap<EquivalenceClass, Double>)m_cands.clone();
    other.m_scoredCands =  (m_numBest < 0) ? new LinkedList<ScoredCandidate>() : new ArrayList<ScoredCandidate>(m_numBest + 1);
    
    for (int i = 0; i < m_scoredCands.size(); i++)
    { other.m_scoredCands.add(m_scoredCands.get(i));
    }
    
    return other;
  }
  
  public boolean smallerScoresAreBetter()
  {
    return m_smallerIsBetter;
  }
  
  /**
   * Attempt to add another candidate into the candidate list. Will not update
   * the existing candidate with a different score.
   * 
   * @param candidate candidate EquivalenceClass
   * @param score candidate's score
   */
  public boolean add(EquivalenceClass candidate, double score)
  {
    boolean added = false;
    
    if (m_cands.containsKey(candidate))
    { LOG.warn("Attempting to add an existing candidate " + candidate.toString() + " to the list for " + m_eq.toString() + ", ignoring.");
    }
    else
    {
      ScoredCandidate newCand = new ScoredCandidate(score, candidate);
      int insIdx = Collections.binarySearch(m_scoredCands, newCand);
      int listSize = m_scoredCands.size();
    
      if (insIdx < 0)
      { insIdx = -(insIdx + 1);
      }
    
      // If there is room or it's better than the current worst scoring element - insert
      if (m_numBest < 0 || listSize < m_numBest || insIdx < listSize)
      {
        m_cands.put(candidate, score);
        m_scoredCands.add(insIdx, newCand);
        added = true;
        
        // Trim the size if too large - drop the last (worst) element.
        if (listSize == m_numBest)
        { 
          m_cands.remove(m_scoredCands.get(listSize).m_candidate);
          m_scoredCands.remove(listSize);
        }
      }
    }
    
    return added;
  }
  
  public boolean containsCandidate(EquivalenceClass cand)
  {
    return (cand != null) && (m_cands.containsKey(cand));
  }

  public void threshold(double lower, double upper)
  {
    ScoredCandidate scoredCand;
    
    for (int idx = (m_scoredCands.size() - 1); idx >= 0; idx--)
    {
      scoredCand = m_scoredCands.get(idx);
   
      if (scoredCand.m_score < lower || scoredCand.m_score > upper)
      {
        m_cands.remove(scoredCand.m_candidate);
        m_scoredCands.remove(idx);
      }
    }
  }
  
  public void retainTop(int k)
  {
    if (k >= 0 && k < m_scoredCands.size())
    {
      for (int idx = m_scoredCands.size() - 1; idx >= k; idx--)
      {
        m_cands.remove(m_scoredCands.get(idx).m_candidate);
        m_scoredCands.remove(idx);
      }
    }
  }
  
  public void clear()
  {
    m_scoredCands.clear();
    m_cands.clear();
  }
  
  /**
   * @return the source English EquivalenceClass for which the candidate list is 
   *   created. 
   */
  public EquivalenceClass getEqClass()
  {
    return m_eq;
  }
  
  public Set<EquivalenceClass> getCandidates()
  {
    return Collections.unmodifiableSet(m_cands.keySet());
  }

  public List<ScoredCandidate> getScoredCandidates()
  {
    return Collections.unmodifiableList(m_scoredCands);
  }
  
  public Double scoreOf(EquivalenceClass candidate)
  {
    return m_cands.get(candidate);
  }
  
  public int rankOf(EquivalenceClass candidate)
  {
    int rank = -1;
    
    if (m_cands.containsKey(candidate))
    { rank = m_scoredCands.indexOf(new ScoredCandidate(m_cands.get(candidate), candidate));
    }
    
    return rank;
  }
  
  public int numInTopK(Collection<EquivalenceClass> cands, int k)
  {
    int rank;
    int num = 0;
    
    for (EquivalenceClass eq : cands)
    {
      if ((rank = rankOf(eq)) >= 0 && rank < k)
      { num++;
      }
    }
    
    return num;
  }
  
  /**
   * @return Best scoring candidate
   */
  public EquivalenceClass getBestCandidate()
  { 
    return (m_scoredCands.size() == 0) ? null : m_scoredCands.get(0).getCandidate();
  }
  
  public EquivalenceClass getRandomCandidate()
  {
    return (m_scoredCands.size() == 0) ? null : m_scoredCands.get(m_rand.nextInt(m_scoredCands.size())).m_candidate;
  }
  
  /**
   * @return best score or null if list is empty
   */
  public Double getBestScore()
  {
    return (m_scoredCands.size() == 0) ? null : m_scoredCands.get(0).getScore();
  }
  
  /**
   * @return number of candidates in the list
   */
  public int numCandidates()
  {
    return m_scoredCands.size();
  }
  
  /**
   * @return String representation of the object
   */
  public String toString()
  {
    StringBuffer strBuf = new StringBuffer();
    
    strBuf.append("<");
    strBuf.append(m_eq.toString());
    strBuf.append(">\n");       

    for (ScoredCandidate curCand : m_scoredCands)
    {
      strBuf.append("   ");
      strBuf.append(curCand);
      strBuf.append("\n");      
    }
    
    return strBuf.toString();
  }
  
  /** List sorted according to score */
  protected List<ScoredCandidate> m_scoredCands;
  protected HashMap<EquivalenceClass, Double> m_cands;
  protected boolean m_smallerIsBetter;
  protected EquivalenceClass m_eq;  
  protected int m_numBest;
  protected Random m_rand;
  
  /**
   * Manages a candidate / score pair.
   */
  public class ScoredCandidate implements Comparable<ScoredCandidate>
  {
    public ScoredCandidate(double score, EquivalenceClass candidate)
    {
      if (candidate == null)
      { throw new IllegalArgumentException("No candidate EquivalenceClass specified.");
      }
      
      m_candidate = candidate;
      m_score = score;
    }
    
    public Double getScore()
    { return m_score;
    }
    
    public EquivalenceClass getCandidate()
    { return m_candidate;
    }
    
    public String toString()
    { return "[" + m_score + "], " + m_candidate;
    }
    
    /**
     * Used to sort the candidates in descending order (best scoring candidates
     * first).
     */
    public int compareTo(ScoredCandidate otherCand)
    {
      int score = 0;
      
      if (m_score > otherCand.m_score)
      { score = m_smallerIsBetter ? 1 : -1;
      }
      else if (m_score < otherCand.m_score)
      { score = m_smallerIsBetter ? -1 : 1;
      }
      else
      { score = m_candidate.compareTo(otherCand.m_candidate);
      }
      
      return score;
    }
    
    public boolean equals(Object obj)
    {
      return (obj instanceof ScoredCandidate) && (0 == compareTo((ScoredCandidate)obj));
    }
    
    protected EquivalenceClass m_candidate;
    protected double m_score;
  }
}
