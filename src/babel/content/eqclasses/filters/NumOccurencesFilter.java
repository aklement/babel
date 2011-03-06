package babel.content.eqclasses.filters;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.number.Number;

public class NumOccurencesFilter implements EquivalenceClassFilter
{
  public NumOccurencesFilter(int num, boolean keepIfMoreThen)
  {
    m_num = num;
    m_keepIfMoreThen = keepIfMoreThen;
  }
  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    if (m_num < 0)
    { return true;
    }
    
    Number number = (eqClass != null) ? (Number)eqClass.getProperty(Number.class.getName()) : null;
    double num = (number == null) ? 0 : number.getNumber();
    
    return (m_keepIfMoreThen && num > m_num) || (!m_keepIfMoreThen && num <= m_num);
  }
  
  protected int m_num;
  protected boolean m_keepIfMoreThen;
}
