package babel.ranking;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;

/**
 * A simple rank aggrgeator - sorts according to aggregate MRR.
 */
public class MRRAggregator
{
  public static final Log LOG = LogFactory.getLog(MRRAggregator.class);

  /** 
   * Note: Assumes all of the collections rank candidates for the same set of
   * source equivalence classes.
   */
  public Collection<EquivClassCandRanking> aggregate(Collection<Collection<EquivClassCandRanking>> rankings)
  {
    if (LOG.isInfoEnabled())
    { LOG.info("Aggregating " + rankings.size() + " rankings.");
    }
    
    // Re-shuffle rankings into something more digestable
    HashMap<EquivalenceClass, HashSet<EquivClassCandRanking>> map = prepare(rankings);
    ArrayList<EquivClassCandRanking> agg = new ArrayList<EquivClassCandRanking>(map.size());    
    
    // Go through all src eq classes and compute MRR for candidates from all rankings
    for (EquivalenceClass src : map.keySet())
    { agg.add(aggregateForOneSrc(src, map.get(src)));
    }
    
    return agg;
  }
  
  protected HashMap<EquivalenceClass, HashSet<EquivClassCandRanking>> prepare(Collection<Collection<EquivClassCandRanking>> rankings)
  {
    HashMap<EquivalenceClass, HashSet<EquivClassCandRanking>> map = new HashMap<EquivalenceClass, HashSet<EquivClassCandRanking>>();
    HashSet<EquivClassCandRanking> curCands;
    
    for (Collection<EquivClassCandRanking> ranking : rankings)
    {
      for (EquivClassCandRanking cands : ranking)
      {
        if (null == (curCands = map.get(cands.m_eq)))
        { map.put(cands.m_eq, curCands = new HashSet<EquivClassCandRanking>());
        }
        
        curCands.add(cands);
      }
    }
    
    return map;
  }
  
  protected EquivClassCandRanking aggregateForOneSrc(EquivalenceClass src, HashSet<EquivClassCandRanking> candRankings)
  {
    EquivClassCandRanking agg = new EquivClassCandRanking(src, -1, false);
    HashSet<EquivalenceClass> allCands = new HashSet<EquivalenceClass>();
    
    // First, collect all candidates from all rankings for the source
    for (EquivClassCandRanking candRanking : candRankings)
    { allCands.addAll(candRanking.getCandidates());
    }
    
    // Compute an MRR score for each one of them
    double mrr = 0.0;
    double rr;
    
    for (EquivalenceClass cand : allCands)
    {
      mrr = 0.0;
      
      for (EquivClassCandRanking ranking : candRankings)
      {
        if ((rr = ranking.rankOf(cand)) < 0)
        { rr = 0.0;
        }
        else
        { rr = 1.0 / rr;
        }
        
        mrr += rr;
      }

      mrr /= (double)candRankings.size();  
      agg.add(cand, mrr);
    }
    
    return agg;
  }
}
