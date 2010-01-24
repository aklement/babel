package babel.content.eqclasses.properties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;

import babel.content.eqclasses.comparators.OverlapComparator;
import babel.content.eqclasses.comparators.NumberComparator;

/**
 * Bag of EquivalenceClass in the context around NE/candidate EquivalenceClass.
 */
public class Context extends Property
{ 
  protected static final int BAG_SIZE = 20;
  protected static final int BAG_MAX = 250;
    
  protected static final Comparator<EquivalenceClass> OVERLAP_COMPARATOR = new OverlapComparator();
  protected static final Comparator<EquivalenceClass> NUMBER_COMPARATOR = new NumberComparator(false);
  
  public Context()
  {
    m_neighborIds = new ArrayList<Integer>();
  }
  
  public List<Integer> getNeighborIds()
  {
    return m_neighborIds;
  }

  /**
   * @return true iff the word's equiv. class had not been seen in this context
   * before
   */
  boolean addContextWord(Class<? extends EquivalenceClass> equivClass, boolean caseSensitive, String contextWord)
  {
    EquivalenceClass tmpEq;
    boolean newContextEq = false;
    int index;

    if (m_neighbors == null)
    { m_neighbors = new ArrayList<EquivalenceClass>();
    }
    
    if ((contextWord != null) && (contextWord.trim().length() > 0) && m_neighbors.size() <= BAG_MAX )
    {
      try
      {
        tmpEq = equivClass.newInstance();
        tmpEq.init(-1, contextWord, caseSensitive);
        
        // Look in top neighbor list
        if ((index = Collections.binarySearch(m_neighbors, tmpEq, OVERLAP_COMPARATOR)) >= 0)
        { // Found it - add the word to the existing equivalence class
          (tmpEq = m_neighbors.get(index)).addMorph(contextWord);
        }
        else
        { // Didn't find it - add to the list
          m_neighbors.add(-(index + 1), tmpEq);
          newContextEq = true;
        }
        
        // Keep track of counts
        Number count = (Number)tmpEq.getProperty(Number.class.getName());
          
        if (count == null)
        { tmpEq.setProperty(count = new Number());
        }
        count.increment();
        
      }
      catch (Exception e)
      { throw new IllegalStateException(e.toString());
      }
    }
    
    return newContextEq;
  }
  
  void sortTrimAndLookup(double threshold, List<EquivalenceClass> allEqs)
  {
    // Sort in the descending order
    Collections.sort(m_neighbors, NUMBER_COMPARATOR);
    
    // Extrtact top occuring eq classes
    if (m_neighbors.size() > BAG_SIZE)
    {
      m_neighbors.subList(Math.min(m_neighbors.size(), BAG_SIZE), m_neighbors.size()).clear();
    }
    
    // Remove context eq classes with score <= threshold (if non-negative)
    if (threshold >= 0)
    {
      SimpleEquivalenceClass tmpEq = new SimpleEquivalenceClass();
      tmpEq.init(-1, "none", false);
      tmpEq.setProperty(new Number(threshold));
      
      int index = Collections.binarySearch(m_neighbors, tmpEq, NUMBER_COMPARATOR);
      
      m_neighbors.subList((index < 0) ? -index-1 : index, m_neighbors.size()).clear();
    }
    
    // Look up all of the context eq in the repository and keep their repository ids
    int curIdx;
    
    for (EquivalenceClass curNeighbor : m_neighbors)
    {
      if ((curIdx = Collections.binarySearch(allEqs, curNeighbor, OVERLAP_COMPARATOR)) >= 0)
      { m_neighborIds.add(new Integer(((EquivalenceClass)allEqs.get(curIdx)).getId()));
      }
    }
    
    m_neighbors = null;
  }
  
  ArrayList<EquivalenceClass> m_neighbors;
  protected ArrayList<Integer> m_neighborIds;
}