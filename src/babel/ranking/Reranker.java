package babel.ranking;

import java.util.Collection;
import java.util.ArrayList;

import babel.ranking.scorers.Scorer;

public class Reranker
{
  public Reranker(Scorer scorer)
  {
    m_ranker = new Ranker(scorer, -1, 1);
  }
  
  public Reranker(Scorer scorer, double threshold)
  {
    m_ranker = new Ranker(scorer, -1, threshold, 1);
  }
  
  public Collection<EquivClassCandRanking> reRank(Collection<EquivClassCandRanking> candidates)
  {
    ArrayList<EquivClassCandRanking> rankings = new ArrayList<EquivClassCandRanking>(candidates.size());    
    
    for (EquivClassCandRanking orig : candidates)
    { rankings.add(m_ranker.getBestCandList(orig.m_eq, orig.getCandidates()));
    }
    
    return rankings;
  }

  /** Ranker to use for re-ranking. */
  protected Ranker m_ranker;
}
