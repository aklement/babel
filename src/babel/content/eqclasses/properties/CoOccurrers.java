package babel.content.eqclasses.properties;

import java.util.HashSet;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;

public class CoOccurrers extends Property
{
  public CoOccurrers()
  {
    m_coOccurers = new HashSet<EquivalenceClass>();
  }
  
  public boolean addCoOccurrer(EquivalenceClass eq)
  {
    return m_coOccurers.add(eq);
  }
  
  public int numCoOccurrers()
  {
    return m_coOccurers.size();
  }
  
  public Set<EquivalenceClass> getCoOccurrers()
  {
    return m_coOccurers;
  }

  protected HashSet<EquivalenceClass> m_coOccurers;

  public String persistToString()
  {
    // TODO: Finish
    return null;
  }

  public boolean unpersistFromString(String str)
  {
    // TODO: Finish
    return false;
  }
}
