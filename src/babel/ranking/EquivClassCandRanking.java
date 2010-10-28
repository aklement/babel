package babel.ranking;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.util.dict.Dictionary;

/**
 * Simple container providing a mechanism for associating scored target 
 * candidates with a source EquivalenceClass object.
 */
public class EquivClassCandRanking
{
  protected static final Log LOG = LogFactory.getLog(EquivClassCandRanking.class);
  protected static final String DEFAULT_ENCODING = "UTF-8";
  
  /**
   * @param EquivalenceClass source EquivalenceClass object
   * @param numBest maximum number of candidates to keep (negative => no limit)
   * @param smallerIsBetter the smaller the score the better
   */
  public EquivClassCandRanking(EquivalenceClass eqClass, int numBest, boolean smallerIsBetter)
  {
    if (eqClass == null)
    { throw new IllegalArgumentException("EquivalenceClass is null.");
    }
    
    m_eq = eqClass;
    m_numBest = numBest;
    m_scoreComparator = new ScoreComparator(smallerIsBetter);    
    m_scoredCandSets = (numBest < 0) ? new LinkedList<ScoredCandidateSet>() : new ArrayList<ScoredCandidateSet>(numBest + 1);
    m_cands = new HashMap<EquivalenceClass, Double>();
    m_rand = new Random(1); 
  }
  
  public EquivClassCandRanking clone()
  {
    EquivClassCandRanking other = new EquivClassCandRanking(m_eq, m_numBest, smallerScoresAreBetter());
    
    for (int i = 0; i < m_scoredCandSets.size(); i++)
    { other.m_scoredCandSets.add(m_scoredCandSets.get(i).clone());
    }
    
    for (EquivalenceClass candidate : m_cands.keySet())
    { other.m_cands.put(candidate, m_cands.get(candidate));
    }
    
    return other;
  }
  
  public boolean smallerScoresAreBetter()
  { return m_scoreComparator.m_smallerIsBetter;
  }
  
  /**
   * Attempt to add another candidate. Note: will not update the existing
   * candidate with a different score.
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
      ScoredCandidateSet candSet = new ScoredCandidateSet(score, candidate);
      int insIdx = Collections.binarySearch(m_scoredCandSets, candSet, m_scoreComparator);
      boolean newSet;
      
      if (newSet = (insIdx < 0))
      { insIdx = -(insIdx + 1);
      }
    
      if (m_numBest < 0 || m_numBest > m_cands.size() || insIdx < m_scoredCandSets.size())
      {
        // Either add to an existing candidate set or create/add a new one
        if (newSet)
        { m_scoredCandSets.add(insIdx, candSet);
        }
        else
        { m_scoredCandSets.get(insIdx).add(candidate);
        }
        
        m_cands.put(candidate, score);
        added = true;
        
        // Trim the size if too large
        if (m_cands.size() == m_numBest + 1)
        {
          candSet = m_scoredCandSets.get(m_scoredCandSets.size() - 1);
          EquivalenceClass cand = candSet.randCandidate();
          candSet.remove(cand);
          m_cands.remove(cand);
          
          if (candSet.size() == 0)
          { m_scoredCandSets.remove(candSet);
          }
        }
      }
    }
    
    return added;
  }

  public boolean containsCandidate(EquivalenceClass cand)
  { return (cand != null) && (m_cands.containsKey(cand));
  }

  public void threshold(double lower, double upper)
  {
    ScoredCandidateSet candSet;
    
    for (int idx = (m_scoredCandSets.size() - 1); idx >= 0; idx--)
    {
      candSet = m_scoredCandSets.get(idx);
   
      if (candSet.m_score < lower || candSet.m_score > upper)
      {
        for (EquivalenceClass eq : candSet.m_candidates)
        { m_cands.remove(eq);
        }
        
        m_scoredCandSets.remove(idx);
      }
    }
  }
  
  public void clear()
  {
    m_scoredCandSets.clear();
    m_cands.clear();
  }
 
  public EquivalenceClass getEqClass()
  { return m_eq;
  }
  
  public Set<EquivalenceClass> getCandidates()
  { return Collections.unmodifiableSet(m_cands.keySet());
  }

  public Map<EquivalenceClass, Double> getScoredCandidates()
  { return Collections.unmodifiableMap(m_cands);
  }
  
  /**
   * Note: more expensive than getScoredCandidates().
   * @return a map sorted by scores in descending order.
   */
  public LinkedHashMap<EquivalenceClass, Double> getOrderedScoredCandidates()
  {
    LinkedHashMap<EquivalenceClass, Double> map = new LinkedHashMap<EquivalenceClass, Double>();
   
    for (ScoredCandidateSet candSet : m_scoredCandSets)
    {
      for (EquivalenceClass eq : candSet.m_candidates)
      { map.put(eq, candSet.m_score);
      }
    }
    
    return map;
  }
  
  public Double scoreOf(EquivalenceClass candidate)
  { return m_cands.get(candidate);
  }
  
  
  /**
   * @return rank of the candidate (1 - smallest rank), or -1 if it was not 
   * found.
   */
  public double rankOf(EquivalenceClass candidate)
  {
    double rank = -1;
    
    if (m_cands.containsKey(candidate))
    {
      rank = 0;
      double curSize = 0;
      
      for (int i = 0; i < m_scoredCandSets.size(); i++)
      {
        curSize = m_scoredCandSets.get(i).size();
        
        if (!m_scoredCandSets.get(i).contains(candidate))
        { 
          rank += curSize;
        }
        else
        {
          rank += (curSize + 1.0) / 2.0;
          break;
        }
      }      
    }
    
    return rank;
  }
  
  /**
   * @return Number of candidates in cands with rank k or higher.
   */
  public int numInTopK(Collection<EquivalenceClass> cands, int k)
  {
    double rank;
    int num = 0;
    
    for (EquivalenceClass eq : cands)
    {
      if ((rank = rankOf(eq)) > 0 && rank <= k)
      { num++;
      }
    }
    
    return num;
  }

  /**
   * @return a random candidate from the best scoring candidate set.
   */
  public EquivalenceClass getBestCandidate()
  { return (m_scoredCandSets.size() == 0) ? null : m_scoredCandSets.get(0).randCandidate();
  }
  
  /**
   * @return a random candidate.
   */
  public EquivalenceClass getRandomCandidate()
  { return (m_scoredCandSets.size() == 0) ? null : m_scoredCandSets.get(m_rand.nextInt(m_scoredCandSets.size())).randCandidate();
  }
  
  /**
   * @return best score or null if list is empty
   */
  public Double getBestScore()
  { return (m_scoredCandSets.size() == 0) ? null : m_scoredCandSets.get(0).score();
  }
  
  /**
   * @return number of candidates in the list
   */
  public int numCandidates()
  { return m_cands.size();
  }
  
  public String toString()
  {
    StringBuffer strBuf = new StringBuffer();
    
    strBuf.append("<");
    strBuf.append(m_eq.toString());
    strBuf.append(">\n");       

    for (ScoredCandidateSet curSet : m_scoredCandSets)
    {
      strBuf.append("   ");
      strBuf.append(curSet);
      strBuf.append("\n");      
    }
    
    return strBuf.toString();
  }
  
  protected void flagTranslations(Collection<EquivalenceClass> translations)
  {
    for (ScoredCandidateSet set : m_scoredCandSets)
    { set.flagIfContainsAnyCandidates(translations);
    }
  }
  
  public static void dumpToFile(Dictionary dict, Collection<EquivClassCandRanking> candRankings, String fileName) throws Exception
  {
    
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), DEFAULT_ENCODING));    
    Set<EquivalenceClass> translations;
    
    for (EquivClassCandRanking candRanking : candRankings)
    {
      if (null != (translations = dict.translate(candRanking.m_eq)))
      { candRanking.flagTranslations(translations);
      }
      
      writer.write(candRanking.toString());
      writer.newLine();
    }
    
    writer.close();
  }
  
  /** List of scored cand sets (kept sorted according to score). */
  protected List<ScoredCandidateSet> m_scoredCandSets;
  /** Map of each candidate to its score for fast lookuo. */
  protected HashMap<EquivalenceClass, Double> m_cands;
  /** Used to keep m_scoredCandSets sorted. */
  protected ScoreComparator m_scoreComparator;
  /** Source equivalence class. */
  protected EquivalenceClass m_eq;
  /** Number of candidates to keep (negative => no limit). */
  protected int m_numBest;
  protected Random m_rand;
  
  /**
   * Keeps a set of candidates assigned the same score.
   */
  public class ScoredCandidateSet
  {
    private ScoredCandidateSet(double score)
    {
      m_candidates = new LinkedHashSet<EquivalenceClass>();
      m_score = score;
      m_flag = false;
    }
    
    public ScoredCandidateSet(double score, EquivalenceClass candidate)
    {
      this(score);
      add(candidate);
    }
    
    public ScoredCandidateSet clone()
    {
      ScoredCandidateSet set = new ScoredCandidateSet(m_score);
      set.m_flag = m_flag;

      for (EquivalenceClass candidate : m_candidates)
      { set.add(candidate);
      }
            
      return set;
    }
    
    public boolean add(EquivalenceClass candidate)
    {
      if (candidate == null)
      { throw new IllegalArgumentException("No candidate EquivalenceClass specified.");
      }
      
      return m_candidates.add(candidate);
    }
    
    boolean remove(EquivalenceClass candidate)
    { return m_candidates.remove(candidate);
    }
    
    public void flagIfContainsAnyCandidates(Collection<EquivalenceClass> cands)
    {
      m_flag = false;
      
      for (EquivalenceClass cand : cands)
      {
        if (m_candidates.contains(cand))
        {
          m_flag = true;
          break;
        }
      }
    }
    
    public boolean contains(EquivalenceClass cand)
    { return m_candidates.contains(cand);
    }
    
    public Double score()
    { return m_score;
    }
    
    public Set<EquivalenceClass> candidates()
    { return Collections.unmodifiableSet(m_candidates);
    }
    
    public EquivalenceClass randCandidate()
    {
      int idx = m_rand.nextInt(m_candidates.size());
      Iterator<EquivalenceClass> iter = m_candidates.iterator();
      EquivalenceClass eq = null;
      
      for (int i = 0; i <= idx; i++)
      { eq = iter.next();
      }
      
      return eq;
    }
    
    public int size()
    { return m_candidates.size();
    }
    
    public String toString()
    { 
      StringBuilder strBld = new StringBuilder((m_flag ? "* " : "") + "[" + m_score + "]");
      
      for (EquivalenceClass eq : m_candidates)
      {
        strBld.append(" ");
        strBld.append(eq.toString());
      }
      
      return strBld.toString();
    }
    
    protected LinkedHashSet<EquivalenceClass> m_candidates;
    protected double m_score;
    protected boolean m_flag;
  }
  
  /**
   * Used to sort the candidate sets in descending according to their scores
   * (best scoring candidates first).
   */
  protected class ScoreComparator implements Comparator<ScoredCandidateSet>
  {
    public ScoreComparator(boolean smallerIsBetter)
    { m_smallerIsBetter = smallerIsBetter;
    }

    public boolean smallerScoresAreBetter()
    { return m_smallerIsBetter;
    }
    
    public int compare(ScoredCandidateSet setOne, ScoredCandidateSet setTwo)
    {
      int score = 0;
      
      if (setOne.m_score > setTwo.m_score)
      { score = m_smallerIsBetter ? 1 : -1;
      }
      else if (setOne.m_score < setTwo.m_score)
      { score = m_smallerIsBetter ? -1 : 1;
      }
      
      return score;
    }

    protected boolean m_smallerIsBetter;
  }
}