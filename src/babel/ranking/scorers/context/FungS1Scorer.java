package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.content.eqclasses.properties.Type.EqType;
import babel.util.dict.Dictionary;

public class FungS1Scorer extends DictScorer
{
  public FungS1Scorer(Dictionary dict, double srcMaxCount, double trgMaxCount)
  {
    super(dict);
    
    m_srcMaxCount = srcMaxCount;
    m_trgMaxCount = trgMaxCount;

    if (m_srcMaxCount == 0 | m_trgMaxCount == 0)
    { throw new IllegalArgumentException("Max count is zero");
    }
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
