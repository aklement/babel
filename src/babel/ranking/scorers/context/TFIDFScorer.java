package babel.ranking.scorers.context;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.context.Context;
import babel.content.eqclasses.properties.context.Context.ContextualItem;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.util.dict.Dictionary;

public class TFIDFScorer extends DictScorer
{
  public static final Log LOG = LogFactory.getLog(TFIDFScorer.class);

  public TFIDFScorer(Dictionary dict, double srcMaxCount, double trgMaxCount)
  {
    super(dict);
    
    m_srcMaxCount = srcMaxCount;
    m_trgMaxCount = trgMaxCount;

    if (m_srcMaxCount == 0 | m_trgMaxCount == 0)
    { throw new IllegalArgumentException("Max count is zero");
    }
  }
  
  public double score(EquivalenceClass srcEq, EquivalenceClass trgEq)
  {
    Context srcContext = (Context)srcEq.getProperty(Context.class.getName());     
    Context trgContext = (Context)trgEq.getProperty(Context.class.getName()); 

    if (srcContext == null || trgContext == null || !srcContext.areContItemsScored() || !trgContext.areContItemsScored())
    { throw new IllegalArgumentException("At leas one of the classes has no or unscored context.");
    }
    
    double score = 0;
    ContextualItem trgCi;
    
    for (ContextualItem srcCi : srcContext.getContextualItems())
    {
      if (null != (trgCi = trgContext.getContextualItem(srcCi.getContextEqId())))
      {
        score += srcCi.getScore() * trgCi.getScore();        
      }
    }
    
    return score;
  }
  
  protected double scoreContItem(ContextualItem contItem, EqType type)
  {
    double tf = contItem.getContextCount();
    double idf = Math.log((type.equals(Type.EqType.SOURCE) ? m_srcMaxCount : m_trgMaxCount) / contItem.getCorpusCount()) + 1.0;
    
    return tf * idf;
  }
  
  protected double m_srcMaxCount;
  protected double m_trgMaxCount;
}
