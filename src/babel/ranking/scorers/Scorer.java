package babel.ranking.scorers;

import babel.content.eqclasses.EquivalenceClass;

public abstract class Scorer
{
  public abstract boolean smallerScoresAreBetter();
  
  /** Must be able to handle concurrent calls. */
  public abstract double score(EquivalenceClass oneEq, EquivalenceClass twoEq);
  
  public String toString()
  { return getClass().getName();
  }
}