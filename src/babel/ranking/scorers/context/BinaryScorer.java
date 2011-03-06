package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.context.Context.ContextualItem;
import babel.content.eqclasses.properties.type.Type.EqType;
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