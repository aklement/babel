package babel.content.eqclasses.collectors;

import java.io.InputStreamReader;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.filters.EquivalenceClassFilter;

/**
 * Collects / prunes EquivalenceClass from an input stream. 
 */
public abstract class EquivalenceClassCollector
{
  public EquivalenceClassCollector(String eqClassName, boolean caseSensitive)
  {
    this(eqClassName, null, caseSensitive);
  }
  
  /**
   * @param EqClassName name of a EquivalenceClass class
   * @param caseSensitive whether or not make EquivalenceClass case sensitive
   */
  @SuppressWarnings("unchecked")
  public EquivalenceClassCollector(String eqClassName, List<EquivalenceClassFilter> filters, boolean caseSensitive)
  {
    m_eqCaseSensitive = caseSensitive;
    m_filters = filters;
    
    try
    { m_eqClass = (Class<? extends EquivalenceClass>)Class.forName(eqClassName);
    }
    catch(Exception e)
    { throw new IllegalArgumentException(e.toString());
    }
  }
  
  /**
   * Collects EquivalenceClass of a specific kind from the given corpus.  Only
   * keeps those equivalence classes which passed the supplied filters (if any).
   * 
   * @param corpusReader Corpus from which to collect
   * @param maxEquivalenceClass maximum number of eq classes to extract (-1 if all)
   * @return map between an equivalence class stem and the corresponding equivalence class
   * @throws Exception 
   */
  public abstract Set<EquivalenceClass> collect(InputStreamReader corpusReader, int maxEquivalenceClass) throws Exception;

  public static Set<EquivalenceClass> filter(Set<EquivalenceClass> eqs, List<EquivalenceClassFilter> filters)
  {
    HashSet<EquivalenceClass> newEqs = null;
    
    if (eqs != null)
    {
      newEqs = new HashSet<EquivalenceClass>();
      
      for (EquivalenceClass eq : eqs)
      {
        if (keep(eq, filters))
        { newEqs.add(eq);
        }
      }
    }
   
    return newEqs;
  }
  
  /**
   * Checks if an equivalence class should be retained (passes all filters). 
   * Should be called by collect().
   */
  protected static boolean keep(EquivalenceClass eq, List<EquivalenceClassFilter> filters)
  {
    boolean keep = true;
    
    if (filters != null)
    {
      for (EquivalenceClassFilter filter : filters)
      {
        if (!(keep = filter.acceptEquivalenceClass(eq)))
        { break;
        }
      }
    }
    
    return keep;
  }
  
  /** Collected EquivalenceClass. */
  protected boolean m_eqCaseSensitive;
  protected Class<? extends EquivalenceClass> m_eqClass;
  protected List<EquivalenceClassFilter> m_filters;
}