package babel.content.eqclasses.collectors;

import java.io.InputStreamReader;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.filters.EquivalenceClassFilter;

/**
 * Collects / prunes EquivalenceClass from an input stream. 
 */
public abstract class EquivalenceClassCollector
{
  protected static int CURRENT_EQCLASS_ID;
  
  static
  {
    CURRENT_EQCLASS_ID = 0;
  }
  
  /**
   * @param EqClassName name of a EquivalenceClass class
   * @param caseSensitive whether or not make EquivalenceClass case sensitive
   */
  public EquivalenceClassCollector(String EqClassName, boolean caseSensitive)
  {
    m_eqs = new ArrayList<EquivalenceClass>();
    m_eqCaseSensitive = caseSensitive;
    try
    { m_eqClass = (Class<? extends EquivalenceClass>)Class.forName(EqClassName);
    }
    catch(Exception e)
    { throw new IllegalArgumentException(e.toString());
    }
  }
  
  /**
   * Collects EquivalenceClass of a specific kind from the given corpus.
   * @param corpusReader Corpus from which to collect
   * @param maxEquivalenceClass maximum number of eq classes to extract (-1 if all)
   * @return number of EquivalenceClass extracted
   */
  public abstract int collect(InputStreamReader corpusReader, int maxEquivalenceClass);
  
  /**
   * Prune the set of EquivalenceClass using the supplied filters.
   * @param filters and array of filters
   */
  public void filter(List<EquivalenceClassFilter> filters)
  {
    EquivalenceClassCollector.filter(m_eqs, filters);
  }

  /**
   * Prune the set of given EquivalenceClass using the supplied filters.
   * @param filters and array of filters
   */
  public static void filter(List<EquivalenceClass> eqs, List<EquivalenceClassFilter> filters)
  {
    if (filters != null)
    {     
      Iterator<EquivalenceClass> eqIter = eqs.iterator();
      Iterator<EquivalenceClassFilter> filterIter;
      EquivalenceClass curEq;
      boolean remove;

      // Apply all filters to all words
      while (eqIter.hasNext())
      {
        curEq = eqIter.next();
        remove = false;
        
        for (filterIter = filters.iterator(); (!remove) && filterIter.hasNext();)
        { remove = !filterIter.next().acceptEquivalenceClass(curEq);
        }
        
        // If at least one filter does not accept - remove 
        if (remove)
        { eqIter.remove();
        }
      }
    }
  }
  
  /**
   * @return the collection of found/filtered EquivalenceClass. 
   */
  public List<EquivalenceClass> getEquivalenceClass()
  {
    return m_eqs;
  }
  
  /**
   * @return the size of the collection.
   */
  public int size()
  {
    return m_eqs.size();
  }
  
  /**
   * Prints all equivalence classes in the collection.
   */
  public void printAllEquivalenceClasses()
  {
    for (EquivalenceClass curEq : m_eqs)
    { System.out.println(curEq);
    }
  }
  
  /** Collected EquivalenceClass. */
  protected ArrayList<EquivalenceClass> m_eqs;
  protected boolean m_eqCaseSensitive;
  protected Class<? extends EquivalenceClass> m_eqClass;
}

