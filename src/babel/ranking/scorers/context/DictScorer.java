package babel.ranking.scorers.context;

import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.ranking.scorers.Scorer;
import babel.util.dict.Dictionary;
import babel.util.dict.Dictionary.DictLookup;

public abstract class DictScorer extends Scorer
{
  public DictScorer()
  {
    m_dict = null;
  }
  
  public void setDict(Dictionary dict)
  {
    m_dict = dict;    
  }
  
  public double score(EquivalenceClass srcEq, EquivalenceClass trgEq)
  {
    Context srcContext = (Context)trgEq.getProperty(Context.class.getName());     
    Context trgContext = (Context)trgEq.getProperty(Context.class.getName()); 

    if (srcContext == null || trgContext == null || !srcContext.contextualItemsScored() || !trgContext.contextualItemsScored())
    { throw new IllegalArgumentException("At leas one of the eq classes has no or unscored context.");
    }
    
    List<DictLookup> translations = m_dict.translateContext(srcEq);
    ContextualItem trgContItem;
    double score = 0, score1 = 0, score2 = 0;
    double w2, w2t, w1;
    
    if (translations != null && trgContext != null)
    {
      for (DictLookup trans : translations)
      {    
        w1 = trans.srcContItem.getScore();
        w2 = 0;
        
        for (EquivalenceClass trgEqClass : trans.trgEqClasses)
        { 
          // If we happen to have more than one translation in target context, pick the best scoring one
          if ((null != (trgContItem = trgContext.lookup(trgEqClass))) && 
              (w2 < (w2t = trgContItem.getScore())))
          {
            w2 = w2t;
          }
        }
        
        score1 += w2 * w2;
        score2 += w1 * w1;
        score += w2 * w1; 
      }
    }
    
    return ((score1 * score2) == 0) ? 0 : score / Math.sqrt(score1 * score2);    
  }
  
  public boolean smallerScoresAreBetter()
  { return false;
  }
  
  public abstract double scoreContItem(ContextualItem contItem);
   
  public void scoreContext(Context context)
  {    
    for (ContextualItem citem : context.getContext())
    { citem.setScore(scoreContItem(citem));
    }
  }
  
  protected Dictionary m_dict;
}
