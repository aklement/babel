package babel.ranking.scorers.edit;

import babel.content.eqclasses.EquivalenceClass;
import babel.ranking.scorers.Scorer;
import babel.util.misc.EditDistance;

/**
 * Computes cosine similarity score between two TimeDistributions.
 */ 
public class EditDistanceScorer extends Scorer
{
  /**
   * Computes edit distance similarity between stems of two equivalence classes.
   *  
   * @param oneEq first EquivalenceClass
   * @param twoEq second EquivalenceClass
   * 
   * @return similarity score
   */
  public double score(EquivalenceClass oneEq, EquivalenceClass twoEq)
  {
    return EditDistance.distance(oneEq.getStem(), twoEq.getStem());
  }

  /**
   * Larger scores mean closer distributions.
   */
  public boolean smallerScoresAreBetter()
  {
    return true;
  }
}