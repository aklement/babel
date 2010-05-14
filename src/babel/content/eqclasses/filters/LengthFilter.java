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
    int length = 0;
    
    if (eqClass != null)
    {
      for (String word : eqClass.getAllWords())
      {
        if (word.length() > length)
        { length = word.length();
        }
      }
    }
    
    return (length > m_size);
  }
  
  protected int m_size;
}