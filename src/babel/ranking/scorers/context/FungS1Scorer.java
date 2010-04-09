package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.content.eqclasses.properties.Type.EqType;

public class FungS1Scorer extends DictScorer
{
  public FungS1Scorer(double srcMaxCount, double trgMaxCount)
  {
    m_srcMaxCount = srcMaxCount;
    m_trgMaxCount = trgMaxCount;

    if (m_srcMaxCount == 0 | m_trgMaxCount == 0)
    { throw new IllegalArgumentException("Max count is zero");
    }
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
