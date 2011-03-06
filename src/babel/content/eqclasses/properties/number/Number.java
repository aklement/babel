package babel.content.eqclasses.properties.number;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

/**
 * Number of occurences property.
 */
public class Number extends Property
{
  public Number()
  {
    this(0);
  }
  
  public Number(long num)
  {
    m_num = num;
  }
  
  public void setNumber(long num)
  {
    m_num = num;
  }
  
  public long getNumber()
  {
    return m_num;
  }
  
  public long increment()
  { 
    return ++m_num;
  }

  public long increment(long delta)
  { 
    return (m_num = m_num + delta);
  }
  
  public long decrement()
  {
    return --m_num;
  }
  
  public String toString()
  {
    return String.valueOf(m_num);
  }
  
  protected long m_num;

  public String persistToString()
  {
    return Long.toString(m_num);
  }

  @Override
  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception
  {
    m_num = Long.parseLong(str);    
  }
}
