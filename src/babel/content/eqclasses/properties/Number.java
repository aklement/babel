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
  
  public String toString()
  {
    return String.valueOf(m_num);
  }
  
  protected double m_num;

  public String persistToString()
  {
    return Double.toString(m_num);
  }

  public boolean unpersistFromString(String str)
  {
    boolean done = false;
    
    try
    { m_num = Double.parseDouble(str);
    }
    catch (Exception e)
    {
      if (LOG.isErrorEnabled())
      {
        LOG.error(e.toString());
      } 
    }
    
    return done;
  }
}
