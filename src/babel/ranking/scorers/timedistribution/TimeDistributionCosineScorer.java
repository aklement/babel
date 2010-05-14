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
		{ throw new IllegalArgumentException("At least one of the arguments doesn't have a distribution property, or they aren't the same size.");
		}
		
		if (!distroOne.isNormalized() || !distroTwo.isNormalized())
    { throw new IllegalArgumentException("At least one of the arguments' distribution property is no normalized.");
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

  /** Normalizes time distributions. */
  public void prepare(EquivalenceClass eq)
  {
    TimeDistribution distro = (TimeDistribution)eq.getProperty(TimeDistribution.class.getName());   

    if (distro == null)
    { throw new IllegalArgumentException("Class has no time distribution property.");
    }
    
    if (!distro.isNormalized())
    { distro.normalize();
    }
  }
}