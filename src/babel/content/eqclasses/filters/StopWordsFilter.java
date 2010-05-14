package babel.content.eqclasses.filters;

import java.util.HashSet;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;

public class StopWordsFilter implements EquivalenceClassFilter
{
  public StopWordsFilter(Set<? extends EquivalenceClass> stopEqs)
  {
    m_stopWords = new HashSet<String>();
    
    for (EquivalenceClass eq : stopEqs)
    {
      for (String word : eq.getAllWords())
      { m_stopWords.add(word);
      }
    }
  }
  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    boolean accept = false;
    
    if (eqClass != null)
    {
      boolean found = false;
      
      for (String word : eqClass.getAllWords())
      {
        if (found = m_stopWords.contains(word))
        { break;
        }
      }
      
      accept = !found;
    }
    
    return accept;
  }

  protected Set<String> m_stopWords;
}
