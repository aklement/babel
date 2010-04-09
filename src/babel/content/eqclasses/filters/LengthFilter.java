package babel.content.eqclasses.filters;

import babel.content.eqclasses.EquivalenceClass;

public class LengthFilter implements EquivalenceClassFilter
{
  public LengthFilter(int size)
  { m_size = size;
  }
  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass) 
  {
    return (eqClass != null) && (eqClass.getStem().length() > m_size);
  }
  
  protected int m_size;
}