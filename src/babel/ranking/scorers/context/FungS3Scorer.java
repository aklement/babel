package babel.ranking.scorers.context;

import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.content.eqclasses.properties.Type.EqType;
import babel.util.dict.Dictionary.DictLookup;

public class FungS3Scorer extends DictScorer
{
  public static final Log LOG = LogFactory.getLog(FungS2Scorer.class);

  // TODO: Do as in S1, just pass the counts instead of the sets
  public FungS3Scorer(Set<EquivalenceClass> srcContextEqs, Set<EquivalenceClass> trgContextEqs)
  {
    m_srcMaxCount = 0;
    m_trgMaxCount = 0;
    Number tmpNum;
    
    // Find max count in both source and target
    for (EquivalenceClass eq : srcContextEqs)
    {
      if (((tmpNum = (Number)eq.getProperty(Number.class.getName())) != null) && (tmpNum.getNumber() > m_srcMaxCount))
      { m_srcMaxCount = tmpNum.getNumber();
      }
    }

    for (EquivalenceClass eq : trgContextEqs)
    {
      if (((tmpNum = (Number)eq.getProperty(Number.class.getName())) != null) && (tmpNum.getNumber() > m_trgMaxCount))
      { m_trgMaxCount = tmpNum.getNumber();
      }
    }
    
    LOG.info("Maximum occurrences: src = " + m_srcMaxCount + ", trg = " + m_trgMaxCount + ".");
    
    if (m_srcMaxCount == 0 | m_trgMaxCount == 0)
    { throw new IllegalArgumentException("Max count is zero");
    }
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
    
    return ((score1 + score2) == 0) ? 0 : score * score / (Math.sqrt(score1 * score2) * (score1 + score2));
  }
  
  public double scoreContItem(ContextualItem contItem)
  {
    EqType type = ((Type)contItem.getContextEq().getProperty(Type.class.getName())).getType();
    double count = ((Number)contItem.getContextEq().getProperty(Number.class.getName())).getNumber();
    
    double tf = contItem.getCount();
    double idf = Math.log((type.equals(Type.EqType.SOURCE) ? m_srcMaxCount : m_trgMaxCount) / count) + 1.0;
    
    return tf * idf;    
  }
  
  protected double m_srcMaxCount;
  protected double m_trgMaxCount;
}