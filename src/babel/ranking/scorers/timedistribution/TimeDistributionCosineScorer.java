package babel.ranking.scorers.timedistribution;

import java.util.HashMap;
import java.util.HashSet;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.time.TimeDistribution;
import babel.ranking.scorers.Scorer;

/**
 * Computes cosine similarity score between two TimeDistributions.
 */ 
public class TimeDistributionCosineScorer extends Scorer
{
  
  public TimeDistributionCosineScorer(int windowSize, boolean slidingWindow)
  {
    m_slidingWindow = slidingWindow;
    m_windowSize = windowSize;
  }
  
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
		
		if ((distroOne == null) || (distroTwo == null))
		{ throw new IllegalArgumentException("At least one of the arguments doesn't have the distribution property.");
		}
		
		if (!distroOne.isNormalized() || !distroTwo.isNormalized())
    { throw new IllegalArgumentException("At least one of the arguments' distribution property is not normalized.");
		}
    
    double result = 0.0;
    HashMap<Integer, Double> oneBins = distroOne.getBins();
    HashMap<Integer, Double> twoBins = distroTwo.getBins(); 

    // Get the intersection of keys
    HashSet<Integer> keys = new HashSet<Integer>(oneBins.keySet());
    keys.retainAll(twoBins.keySet());

    // Compute enumerator
    for (Integer key : keys)
    {
      result += oneBins.get(key) * twoBins.get(key);
    }
    
    // Compute whole coefficient
    double denom = Math.sqrt(distroOne.getSumSquares()) * Math.sqrt(distroTwo.getSumSquares());

    //System.out.println(oneEq.getStem()+" "+twoEq.getStem()+" "+result+" "+denom);
    //System.out.println(distroOne.toString());
    //System.out.println(distroTwo.toString());

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
    
    distro.reBin(m_windowSize, m_slidingWindow);
    distro.normalize();
    
    //System.out.println("Time distro for "+eq.getStem()+": "+distro.toString());
  }
  
  protected boolean m_slidingWindow;
  protected int m_windowSize;
}