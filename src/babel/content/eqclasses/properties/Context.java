package babel.content.eqclasses.properties;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.comparators.OverlapComparator;
import babel.ranking.scorers.context.DictScorer;

/**
 * Bag of EquivalenceClass in the context around NE/candidate EquivalenceClass.
 */
public class Context extends Property
{     
  protected static final Comparator<EquivalenceClass> OVERLAP_COMPARATOR = new OverlapComparator();

  public Context()
  {
    this(null);
  }
  
  /**
   * @param eq equivalence class associated with this context.
   */
  public Context(EquivalenceClass eq)
  {
    m_eq = eq;
    m_neighborsMap = new HashMap<String, ContextualItem>();
    m_allContextCounts = 0;
    m_contItemsScored = false;
  }
  
  public int size()
  {
    return m_neighborsMap.size();
  }
  
  public EquivalenceClass getEq()
  {
    return m_eq;
  }
  
  public Collection<ContextualItem> getContext()
  {
    return m_neighborsMap.values();
  }
  
  public boolean contextualItemsScored()
  {
    return m_contItemsScored;
  }
  
  public void scoreContextualItems(DictScorer scorer)
  {
    scorer.scoreContext(this);
    m_contItemsScored = true;
  }
  
  EquivalenceClass addContextWord(Class<? extends EquivalenceClass> equivClass, boolean caseSensitive, HashMap<String, EquivalenceClass> contextEqsMap, String contextWord)
  {
    EquivalenceClass contextEq = null;
    ContextualItem contextItem = null;
    
    if ((contextWord != null) && (contextWord.trim().length() > 0))
    {
      try
      {
        contextEq = equivClass.newInstance();
        contextEq.init(-1, contextWord, caseSensitive);
        
        // If we already saw it in context - just increment count
        if (null != (contextItem = m_neighborsMap.get(contextEq.getStem())))
        {
          contextEq = contextItem.m_contextEq;
          contextItem.incrementCount();
          m_allContextCounts++;
        }
        // Otherwise, if it is the contextual item from our large list - add it
        else if (null != (contextEq = contextEqsMap.get(contextEq.getStem()))) 
        {
          m_neighborsMap.put(contextEq.getStem(), new ContextualItem(this, contextEq));
          m_allContextCounts++;
        }
        else
        {
          contextEq = null;
        }
      }
      catch (Exception e)
      { throw new IllegalStateException(e.toString());
      }
    }
    
    return contextEq;
  }
  
  public void pruneContext(int numKeep, Comparator<ContextualItem> comparator)
  {    
    if ((m_neighborsMap != null) && (numKeep >= 0) && (m_neighborsMap.size() > numKeep))
    {
      LinkedList<ContextualItem> valList = new LinkedList<ContextualItem>(m_neighborsMap.values());
      ContextualItem val;
      
      // Sort according to a given comparator
      Collections.sort(valList, comparator);
      
      m_neighborsMap.clear();
      
      for (int i = 0; i < Math.min(valList.size(), numKeep); i++)
      {
        val = valList.get(i);
        m_neighborsMap.put(val.m_contextEq.getStem(), val);
      }
    }
  }
  
  public ContextualItem lookup(EquivalenceClass contextEq)
  {    
    return (m_neighborsMap == null) ? null : m_neighborsMap.get(contextEq.getStem());
  }
  
  public String toString()
  {
    return m_neighborsMap.values().toString();
  }
  
  public String persistToString()
  {
    StringBuilder strBld = new StringBuilder();
    boolean first = true;
    
    for (String key : m_neighborsMap.keySet())
    {
      if (first)
      { first = false;
      }
      else
      { strBld.append("\t");
      }
      strBld.append(m_neighborsMap.get(key).persistToString());
    }
    
    return strBld.toString();
  }

  public void unpersistFromString(EquivalenceClass eq, Map<Integer, EquivalenceClass> allEqs, String str) throws Exception
  {
    m_eq = eq;
    m_allContextCounts = 0;    
    m_neighborsMap.clear();
    m_contItemsScored = false;    
    
    if (!str.isEmpty())
    {
      String[] toks = str.split("\t");
      ContextualItem cItem;
    
      for (int i = 0; i < toks.length; i++)
      {
        (cItem = new ContextualItem(this)).unpersistFromString(toks[i], allEqs);
        m_allContextCounts += cItem.m_count;
        m_neighborsMap.put(cItem.m_contextEq.getStem(), cItem);
      }
    }
  }
  
  protected HashMap<String, ContextualItem> m_neighborsMap;
  /** Source equivalence class. */
  protected EquivalenceClass m_eq;
  /** Number of all word occurences in context. */
  protected int m_allContextCounts;
  /** true iff scores have been assigned to the contextual items */
  protected boolean m_contItemsScored;
  
  public static class CountComparator implements Comparator<ContextualItem>
  {
    public int compare(ContextualItem item1, ContextualItem item2)
    {
      return item2.getCount() - item1.getCount();
    }
  }

  public static class ScoreComparator implements Comparator<ContextualItem>
  {
    public ScoreComparator(DictScorer scorer)
    {
      m_scorer = scorer;
    }
    
    public int compare(ContextualItem item1, ContextualItem item2)
    {
      double score1 = m_scorer.scoreContItem(item1);
      double score2 = m_scorer.scoreContItem(item2);
      int direction = m_scorer.smallerScoresAreBetter() ? -1 : 1;
      
      return score1 == score2 ? 0 : direction * (score2 > score1 ? 1 : -1);
    }
    
    protected DictScorer m_scorer;
  }  
  
  public static class EqOverlapComparator implements Comparator<ContextualItem>
  {
    public int compare(ContextualItem item1, ContextualItem item2)
    {
      return OVERLAP_COMPARATOR.compare(item1.m_contextEq, item2.m_contextEq);
    }
  }
  
  public static class ContextualItem
  {
    protected ContextualItem(Context context)
    {
      m_context = context;
      m_contextEq = null;
      m_count = 0;
    }

    public ContextualItem(Context context, EquivalenceClass contextEq)
    {
      this(context, contextEq, 1);
    }
    
    public ContextualItem(Context context, EquivalenceClass contextEq, int count)
    {
      m_contextEq = contextEq;
      m_context = context;
      m_count = count;
    }
    
    public int hashCode()
    {
      return m_contextEq.hashCode();
    }
    
    public void incrementCount()
    {
      m_count++;
    }
    
    public int getCount()
    {
      return m_count;
    }
    
    public Context getContext()
    {
      return m_context;
    }
    
    public void setScore(double score)
    {
      m_score = score;
    }

    public double getScore()
    {
      return m_score;
    }
    
    public EquivalenceClass getContextEq()
    {
      return m_contextEq;
    }
    
    public String toString()
    {
      return m_contextEq.toString() + " (" + m_count + ")"; 
    }
    
    protected String persistToString()
    {
      StringBuilder strBld = new StringBuilder();
      
      strBld.append(m_contextEq.getId());
      strBld.append("("); strBld.append(m_count); strBld.append(")");
      
      return strBld.toString();
    }
    
    public void unpersistFromString(String str, Map<Integer, EquivalenceClass> map) throws Exception
    {
      m_score = 0;
      
      String[] toks = str.split("[()]");

      m_contextEq = map.get(Integer.parseInt(toks[0]));
      m_count = Integer.parseInt(toks[1]); 
    }
    
    protected Context m_context;    
    protected int m_count;
    protected double m_score;
    protected EquivalenceClass m_contextEq;
  }
}