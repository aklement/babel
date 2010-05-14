package babel.ranking.scorers.context;

import java.util.Collection;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.content.eqclasses.properties.Type.EqType;
import babel.ranking.scorers.Scorer;
import babel.util.dict.Dictionary;

public abstract class DictScorer extends Scorer
{
  public DictScorer(Dictionary dict)
  {
    m_dict = dict;
  }
  
  public double score(EquivalenceClass srcEq, EquivalenceClass trgEq)
  {
    Context srcContext = (Context)srcEq.getProperty(Context.class.getName());     
    Context trgContext = (Context)trgEq.getProperty(Context.class.getName()); 

    if (srcContext == null || trgContext == null || !srcContext.areContItemsScored() || !trgContext.areContItemsScored())
    { throw new IllegalArgumentException("At leas one of the classes has no or unscored context.");
    }
    
    double score = 0, score1 = 0, score2 = 0;
    double w2, w1;
    
    ContextualItem trgCi;
    
    for (ContextualItem srcCi : srcContext.getContextualItems())
    {
      w1 = srcCi.getScore();
      w2 = 0;
      
      if (null != (trgCi = trgContext.getContextualItem(srcCi.getContextEqId())))
      {
        w2 = trgCi.getScore();
      }
      
      score1 += w2 * w2;
      score2 += w1 * w1;
      score += w2 * w1;
    }
    
    return ((score1 * score2) == 0) ? 0 : score / Math.sqrt(score1 * score2);        
  }
  
  public boolean smallerScoresAreBetter()
  { return false;
  }
  
  protected abstract double scoreContItem(ContextualItem contItem, EqType type);
  
  /** Projects and precomuptes feature scores. */
  public void prepare(EquivalenceClass eq)
  {
    EqType type = ((Type)eq.getProperty(Type.class.getName())).getType();
    Context context = ((Context)eq.getProperty(Context.class.getName()));

    if (context == null)
    { throw new IllegalArgumentException("Class has no context property.");
    }
    else if (type == null || EqType.NONE.equals(type))
    { throw new IllegalArgumentException("Class is of unknown type, cannot compute scores.");
    }

    // If src, project before scoring
    Collection<ContextualItem> cis = EqType.SOURCE.equals(type) ?  m_dict.translateContext(eq) : context.getContextualItems();
    context.clear();
    double score;
    ContextualItem curCi;
    
    for (ContextualItem ci : cis)
    {
      score = scoreContItem(ci, type);
      ci.setScore(score);

      // Add a contextual item or if we happen to have more than one translation in target context, pick the best scoring one
      if ((null == (curCi = context.getContextualItem(ci.getContextEqId()))) || (curCi != null && curCi.getScore() < score))
      {
        context.setContextualItem(ci);
      }
    }
    
    context.contItemsScored();
  }
  
  protected Dictionary m_dict;
}
