package babel.ranking.scorers.context;

import babel.content.eqclasses.properties.context.Context.ContextualItem;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
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
	//Number of times saw this context with the word type
    double tf = contItem.getContextCount();
    //Maximum number of times any context occurs (with any word) / total number of times this context appears in the corpus
    double idf = Math.log((type.equals(Type.EqType.SOURCE) ? m_srcMaxCount : m_trgMaxCount) / contItem.getCorpusCount()) + 1.0;
    
    return tf * idf;    
  }
  
  protected double m_srcMaxCount;
  protected double m_trgMaxCount;
}
