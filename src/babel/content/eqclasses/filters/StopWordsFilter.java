package babel.content.eqclasses.filters;

import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;

public class StopWordsFilter implements EquivalenceClassFilter
{
  public StopWordsFilter(Set<EquivalenceClass> stopEqs)
  {
    m_stopWords = stopEqs;
  }
  
  @Override
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass)
  {
    return (eqClass != null) && !m_stopWords.contains(eqClass);
  }

  Set<EquivalenceClass> m_stopWords;
}
