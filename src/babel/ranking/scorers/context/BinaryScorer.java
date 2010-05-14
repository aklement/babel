package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.content.eqclasses.properties.Type.EqType;
import babel.util.dict.Dictionary;

public class BinaryScorer extends DictScorer
{
  public BinaryScorer(Dictionary dict)
  { super(dict);
  }

  protected double scoreContItem(ContextualItem contItem, EqType type)
  {return 1.0;
  }
}