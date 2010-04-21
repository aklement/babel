package babel.ranking.scorers.timedistribution;

import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.TimeDistribution;
import babel.ranking.scorers.Scorer;

/**
 * Computes cosine similarity score between two TimeDistributions.
 */ 
public class TimeDistributionCosineScorer extends Scorer
{
  /**
   * Computes cosine similarity coefficient between two distribution vectors.
   * The larger the score - the closer the distributions. Side-effect: may 
   * normalize distribution (if it isn't already).
   *  
   * @param oneEq first EquivalenceClass
   * @param twoEq second EquivalenceClass
   * 
   * @return similarity score
   */
	public double score(EquivalenceClass oneEq, EquivalenceClass twoEq)
	{
		TimeDistribution distroOne = (TimeDistribution)oneEq.getProperty(TimeDistribution.class.getName());
		TimeDistribution distroTwo = (TimeDistribution)twoEq.getProperty(TimeDistribution.class.getName());		
		
		if ((distroOne == null) || (distroTwo == null) ||
				(distroOne.getSize() != distroTwo.getSize()))
		{ throw new IllegalArgumentException("At least one of the EquivalenceClass doesn't have a distribution property, or they aren't the same size.");
		}

    // Normalize them
    if (!distroOne.isNormalized())
    { distroOne.normalize();
    }
    
    if (!distroTwo.isNormalized())
    { distroTwo.normalize();
    }
    
    double result = 0.0;
    List<Double> oneBins = distroOne.getBins();
    List<Double> twoBins = distroTwo.getBins();    
    
    // Compute enumerator
    for (int i = 0; i < distroOne.getSize(); i++)
    {
      result += (oneBins.get(i)).doubleValue() * (twoBins.get(i)).doubleValue();
    }
    
    // Compute whole coefficient
    double denom = Math.sqrt(distroOne.getSumSquares()) * Math.sqrt(distroTwo.getSumSquares());
    result = (denom == 0.0) ? 0.0 : (result / denom);
    
    return result;
	}

  /**
   * Larger scores mean closer distributions.
   */
  public boolean smallerScoresAreBetter()
  {
    return false;
  }
}