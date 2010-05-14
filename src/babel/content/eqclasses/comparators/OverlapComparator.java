package babel.content.eqclasses.comparators;

import java.util.Comparator;

import babel.content.eqclasses.EquivalenceClass;

public class OverlapComparator implements Comparator<EquivalenceClass>
{
  public int compare(EquivalenceClass eqclass1, EquivalenceClass eqclass2)
  {
    return (eqclass1.sameEqClass(eqclass2) ? 0 : eqclass1.compareTo(eqclass2));
  }
}
