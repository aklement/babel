package babel.ranking.scorers.context;

import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.util.dict.Dictionary.DictLookup;

public class CountRatioScorer extends DictScorer
{
  public CountRatioScorer()
  {
    super();
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
    double score = 0;

    if (translations != null && trgContext != null)
    {
      for (DictLookup trans : translations)
      {
        for (EquivalenceClass trgEqClass : trans.trgEqClasses)
        {
          if (null != (trgContItem = trgContext.lookup(trgEqClass)))
          { 
            score += trgContItem.getScore() * trans.srcContItem.getScore();
          }
        }
      }
    }
    
    return score;
  }
  
  public double scoreContItem(ContextualItem contItem)
  {
    return (double) contItem.getCount() / (double) ((Number)contItem.getContextEq().getProperty(Number.class.getName())).getNumber();    
  }
}
