package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.Context.ContextualItem;

public class BinaryScorer extends DictScorer
{
  public BinaryScorer()
  { super();
  }
  
  public double scoreContItem(ContextualItem contItem)
  { return 1.0;    
  }
}