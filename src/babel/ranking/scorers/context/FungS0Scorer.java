package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.context.Context.ContextualItem;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.util.dict.Dictionary;

public class FungS0Scorer extends DictScorer
{
  public FungS0Scorer(Dictionary dict)
  {
    super(dict);
  }
  
  protected double scoreContItem(ContextualItem contItem, EqType type)
  {
    return contItem.getContextCount();
  }
  
  protected double m_srcMaxCount;
  protected double m_trgMaxCount;
}