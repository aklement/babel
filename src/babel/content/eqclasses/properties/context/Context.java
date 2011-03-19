package babel.content.eqclasses.properties.context;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Collections;
import java.util.Comparator;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;
import babel.content.eqclasses.properties.number.Number;
import babel.ranking.scorers.context.DictScorer;

/**
 * Bag of EquivalenceClass in the context around NE/candidate EquivalenceClass.
 */
public class Context extends Property
{
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
    m_neighborsMap = new HashMap<Long, ContextualItem>();
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

  public HashSet<Long> getContextualItemIds()
  {
    return new HashSet<Long>(m_neighborsMap.keySet());
  }
  
  public Collection<ContextualItem> getContextualItems()
  {
    return new ArrayList<ContextualItem>(m_neighborsMap.values());
  }
  
  /** Score of the contextual item with a given id, or 0 if the item is not in 
   * context. */
  public double getContextualItemScore(Long contItemId) {
    ContextualItem contItem = m_neighborsMap.get(contItemId);
    return (contItem == null) ? 0 : contItem.getScore();
  }
  
  public void clear()
  {
    m_neighborsMap.clear();
  }
  
  public ContextualItem getContextualItem(Long contItemId)
  {
    return m_neighborsMap.get(contItemId);
  }
  
  public void setContextualItem(ContextualItem contItem)
  {
    contItem.m_context = this;
    m_neighborsMap.put(contItem.m_contEqID, contItem);
  }
  
  public boolean areContItemsScored()
  {
    return m_contItemsScored;
  }
  
  public void contItemsScored()
  {
    m_contItemsScored = true;
  }
   
  void addContextWord(boolean caseSensitive, HashMap<String, EquivalenceClass> contextEqsMap, String contextWord)
  {
    EquivalenceClass contextEq = null;
    ContextualItem contextItem = null;
    String word = EquivalenceClass.getWordOfAppropriateForm(contextWord, caseSensitive);

    if ((word != null) && (word.length() > 0))
    {
      if (m_collectedMap == null)
      { m_collectedMap = new HashMap<String, ContextualItem>(); 
      }
      
      try
      {
        // If we already saw it in context - just increment count
        if (null != (contextItem = m_collectedMap.get(word)))
        {
          contextItem.incContextCount();
        }
        // Otherwise, if it is the contextual item from our large list - add it
        else if (null != (contextEq = contextEqsMap.get(word))) 
        {
          contextItem = new ContextualItem(this, contextEq);
          
          // Add a map entry for each of the words
          for (String contWord : contextEq.getAllWords())
          { 
            m_collectedMap.put(contWord, contextItem);
          }
          
          m_neighborsMap.put(contextItem.getContextEqId(), contextItem);
        }
      }
      catch (Exception e)
      { throw new IllegalStateException(e.toString());
      }
    }    
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
        m_neighborsMap.put(val.m_contEqID, val);
      }
    }
  }
  
  public ContextualItem lookup(Long contextEqId)
  {    
    return (m_neighborsMap == null) ? null : m_neighborsMap.get(contextEqId);
  }
  
  public String toString()
  {
    return m_neighborsMap.values().toString();
  }
  
  public String persistToString()
  {
    StringBuilder strBld = new StringBuilder();
    boolean first = true;
    
    for (Long key : m_neighborsMap.keySet())
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

  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception
  {
    m_eq = eq;
    m_neighborsMap.clear();
    m_contItemsScored = false;    
    
    if (!str.isEmpty())
    {
      String[] toks = str.split("\t");
      ContextualItem cItem;
    
      for (int i = 0; i < toks.length; i++)
      {
        cItem = ContextualItem.unpersistFromString(this, toks[i]);
        m_neighborsMap.put(cItem.getContextEqId(), cItem);
      }
    }
  }
  
  protected HashMap<Long, ContextualItem> m_neighborsMap;
  /** Source equivalence class. */
  protected EquivalenceClass m_eq;
  /** true iff scores have been assigned to the contextual items */
  protected boolean m_contItemsScored;
  /** Token to contextul item map - only used during collection */
  protected HashMap<String, ContextualItem> m_collectedMap = null; 
  
  public static class CountComparator implements Comparator<ContextualItem>
  {
    public int compare(ContextualItem item1, ContextualItem item2)
    {
      long diff = item2.getContextCount() - item1.getContextCount();
      return diff == 0 ? 0 : (diff < 0 ? -1 : 1);
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
      
      item1.getScore();
      double score1 = item1.getScore();
      double score2 = item2.getScore();
      int direction = m_scorer.smallerScoresAreBetter() ? -1 : 1;
      
      return score1 == score2 ? 0 : direction * (score2 > score1 ? 1 : -1);
    }
    
    protected DictScorer m_scorer;
  }
  
  public static class ContextualItem
  {
    private ContextualItem(Context context)
    {
      m_context = context;
      m_contCount = 0;
      m_corpCount = 0;
      m_score = 0;
    }

    public ContextualItem(Context context, EquivalenceClass contextEq)
    {
      this(context, contextEq.getId(), ((Number)contextEq.getProperty(Number.class.getName())).getNumber(), 1);
    }

    public ContextualItem(Context context, Long contEqId, long corpusCount, long contextCount)
    {
      m_contEqID = contEqId;
      m_context = context;
      m_corpCount = corpusCount;
      m_contCount = contextCount;
      m_score = 0;
    }
    
    public int hashCode()
    {
      return m_contEqID.hashCode();
    }
    
    public void incContextCount()
    {
      m_contCount++;
    }
    
    public long getContextCount()
    {
      return m_contCount;
    }

    public long getCorpusCount()
    {
      return m_corpCount;
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
    
    public Long getContextEqId()
    {
      return m_contEqID;
    }
    
    public String toString()
    {
      return persistToString(); 
    }
    
    protected String persistToString()
    {
      StringBuilder strBld = new StringBuilder();
      
      strBld.append(m_contEqID);
      strBld.append("(");
      strBld.append(m_contCount);
      strBld.append(" ");
      strBld.append(m_corpCount); 
      strBld.append(")");
      
      return strBld.toString();
    }
    
    public static ContextualItem unpersistFromString(Context context, String str) throws Exception
    {
      ContextualItem ci = new ContextualItem(context);
      
      String[] toks = str.split("[( )]");

      ci.m_contEqID = Long.parseLong(toks[0]);
      ci.m_contCount = Long.parseLong(toks[1]); 
      ci.m_corpCount = Long.parseLong(toks[2]);       
      return ci;
    }
    
    protected Context m_context;
    protected long m_contCount;
    protected long m_corpCount;
    protected double m_score;
    protected Long m_contEqID;
  }
}