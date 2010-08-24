package babel.content.eqclasses.comparators;

import java.util.Comparator;

import babel.content.eqclasses.EquivalenceClass;

/**
 * Compares two EquivalenceClasses's stems.
 */
public class LexComparator implements Comparator<EquivalenceClass> 
{
  public LexComparator(boolean ascending)
  {
    m_ascending = ascending;
  }
  
  public int compare(EquivalenceClass eqclass1, EquivalenceClass eqclass2)
  { 
    int ascendingScore = eqclass1.getStem().compareTo(eqclass2.getStem());
    return m_ascending ? ascendingScore : -ascendingScore;    
  }
  
  protected boolean m_ascending;
}