package babel.content.eqclasses.filters;

import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.util.dict.Dictionary;

public class DictionaryFilter implements EquivalenceClassFilter
{
  public DictionaryFilter(Dictionary dict, boolean keepIfFoundInDict, boolean checkSrc)
  {
    m_entries = checkSrc ? dict.getAllKeys() : dict.getAllVals();    
    m_keepIfFoundInDict = keepIfFoundInDict;
  }
  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    boolean found = m_entries.contains(eqClass);
    return (m_keepIfFoundInDict && found) || (!m_keepIfFoundInDict && !found);
  }
  
  protected Set<EquivalenceClass> m_entries;
  protected boolean m_keepIfFoundInDict;
}