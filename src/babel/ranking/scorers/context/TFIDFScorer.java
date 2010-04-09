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

public class TFIDFScorer extends DictScorer
{
  public static final Log LOG = LogFactory.getLog(TFIDFScorer.class);

  public TFIDFScorer(Set<EquivalenceClass> srcContextEqs, Set<EquivalenceClass> trgContextEqs)
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
    List<DictLookup> translations = m_dict.translateContext(srcEq);
    Context trgContext = (Context)trgEq.getProperty(Context.class.getName()); 
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
    EqType type = ((Type)contItem.getContextEq().getProperty(Type.class.getName())).getType();
    double count = ((Number)contItem.getContextEq().getProperty(Number.class.getName())).getNumber();
    
    double tf = contItem.getCount();
    double idf = Math.log((type.equals(Type.EqType.SOURCE) ? m_srcMaxCount : m_trgMaxCount) / count) + 1.0;
    
    return tf * idf;    
  }
  
  protected double m_srcMaxCount;
  protected double m_trgMaxCount;
}
