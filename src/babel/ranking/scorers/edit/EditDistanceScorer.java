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
    double count = 0;
    double dist = 0;
    
    // We'll just compute the average edit distance across all pairs - expensive!  TODO: a better heuristic?
    for (String oneStr : oneEq.getAllWords())
    {
      for (String twoStr : twoEq.getAllWords())
      {
        dist += EditDistance.distance(oneStr, twoStr);
        count++;
      }
    }
   
    return dist / count;
  }

  /**
   * Larger scores mean closer distributions.
   */
  public boolean smallerScoresAreBetter()
  {
    return true;
  }
  
  public void prepare(EquivalenceClass eq) {}
}