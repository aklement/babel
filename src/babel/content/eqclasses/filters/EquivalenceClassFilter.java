package babel.content.eqclasses.filters;

import babel.content.eqclasses.EquivalenceClass;

public interface EquivalenceClassFilter
{
  /** 
   * @param eqClass
   * @return true if an EquivalenceClass object should be accepted. 
   */
  public boolean acceptEquivalenceClass(EquivalenceClass eqClass);
}
