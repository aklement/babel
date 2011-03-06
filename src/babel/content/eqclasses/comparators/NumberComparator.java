package babel.content.eqclasses.comparators;

import java.util.Comparator;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.number.Number;

/**
 * Compares two EquivalenceClasses based on the value of the number property.
 */
public class NumberComparator implements Comparator<EquivalenceClass> 
{
  public NumberComparator(boolean ascending)
  {
    m_ascending = ascending;
  }
  
  public int compare(EquivalenceClass eqclass1, EquivalenceClass eqclass2)
  { 
    double num1 = ((Number)eqclass1.getProperty(Number.class.getName())).getNumber();
    double num2 = ((Number)eqclass2.getProperty(Number.class.getName())).getNumber();    
    int ascendingScore = (num1 == num2) ? 0 : ((num1 < num2) ? -1 : 1);

    return m_ascending ? ascendingScore : -ascendingScore;    
  }
  
  protected boolean m_ascending;
}
