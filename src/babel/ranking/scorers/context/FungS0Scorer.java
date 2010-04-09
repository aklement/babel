package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.Context.ContextualItem;

public class FungS0Scorer extends DictScorer
{
  public FungS0Scorer()
  {
    super();
  }
  
  public double scoreContItem(ContextualItem contItem)
  {
    return contItem.getCount();
  }
  
  protected double m_srcMaxCount;
  protected double m_trgMaxCount;
}