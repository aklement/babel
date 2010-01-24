package babel.content.eqclasses.properties;

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
  
  public Number(double num)
  {
    m_num = num;
  }
  
  public void setNumber(double num)
  {
    m_num = num;
  }
  
  public double getNumber()
  {
    return m_num;
  }
  
  public double increment()
  {
    return ++m_num;
  }
  
  public double decrement()
  {
    return --m_num;
  }
  
  protected double m_num;
}
