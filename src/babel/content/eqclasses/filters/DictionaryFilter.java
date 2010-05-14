package babel.content.eqclasses.filters;

import babel.content.eqclasses.EquivalenceClass;
import babel.util.dict.Dictionary;

public class DictionaryFilter implements EquivalenceClassFilter
{
  public DictionaryFilter(Dictionary dict, boolean keepIfFoundInDict, boolean checkSrc)
  {
    m_keepIfFoundInDict = keepIfFoundInDict;
    m_dict = dict;
    m_src = checkSrc;
  }
  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    boolean found = m_src ? m_dict.containsSrc(eqClass) : m_dict.containsTrg(eqClass);
    return (m_keepIfFoundInDict && found) || (!m_keepIfFoundInDict && !found);
  }
  
  protected Dictionary m_dict;
  protected boolean m_keepIfFoundInDict;
  protected boolean m_src;
}