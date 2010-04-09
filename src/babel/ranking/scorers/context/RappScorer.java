package babel.ranking.scorers.context;

import java.util.List;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.content.eqclasses.properties.Type.EqType;
import babel.util.dict.Dictionary.DictLookup;

public class RappScorer extends DictScorer
{
  public RappScorer(double srcTokCount, double trgTokCount)
  {
    m_srcTokCount = srcTokCount;
    m_trgTokCount = trgTokCount;
  }

  public double score(EquivalenceClass srcEq, EquivalenceClass trgEq)
  {
    Context srcContext = (Context)trgEq.getProperty(Context.class.getName());     
    Context trgContext = (Context)trgEq.getProperty(Context.class.getName()); 

    if (srcContext == null || trgContext == null || !srcContext.contextualItemsScored() || !trgContext.contextualItemsScored())
    { throw new IllegalArgumentException("At leas one of the eq classes has no or unscored context.");
    }
    
    List<DictLookup> translations = m_dict.translateContext(srcEq);
    ContextualItem trgContItem;
    double score = 0;
    double w2, w1, diff, difft;
    
    if (translations != null && trgContext != null)
    {
      for (DictLookup trans : translations)
      {    
        w1 = trans.srcContItem.getScore();
        w2 = 0;
        diff = difft = Double.MAX_VALUE;
        
        for (EquivalenceClass trgEqClass : trans.trgEqClasses)
        { 
          // If we happen to have more than one translation in target context, pick the best scoring one
          if (null != (trgContItem = trgContext.lookup(trgEqClass)))
          {
            difft = Math.abs(w1 - trgContItem.getScore());
            
            if (difft < diff)
            {
              w2 = trgContItem.getScore();
            }
          }
        }
        
        score += Math.abs(w1-w2);
      }
    }
    
    return score;
  }
  
  public double scoreContItem(ContextualItem contItem)
  {
    // A is the word, B is contextual word
    double freqA = (double) ((Number)contItem.getContext().getEq().getProperty(Number.class.getName())).getNumber();
    double freqB = (double) ((Number)contItem.getContextEq().getProperty(Number.class.getName())).getNumber();
    EqType type = ((Type)contItem.getContextEq().getProperty(Type.class.getName())).getType();

    double[][] k = new double[2][2];
    
    k[0][0] = (double) contItem.getCount();
    k[0][1] = freqA - k[0][0];
    k[1][0] = freqB - k[0][0];
    k[1][1] = (type.equals(Type.EqType.SOURCE) ? m_srcTokCount : m_trgTokCount) - freqA - freqB;
    
    double[] C = new double[2];
    C[0] = k[0][0] + k[0][1];
    C[1] = k[1][0] + k[1][1];

    double[] R = new double[2];
    R[0] = k[0][0] + k[1][0];
    R[1] = k[0][1] + k[1][1];
    
    double N = C[0] + C[1];
    double score = 0;
    
    for (int i = 0; i <= 1; i++)
    {
      for (int j = 0; j <= 1; j++)
      { score += k[i][j] * Math.log(k[i][j] * N / (C[i] * R[j])); 
      }
    }
    
    return score;
  }
  
  public void scoreContext(Context context)
  {
    double score;
    double totalScore = 0;
    
    for (ContextualItem citem : context.getContext())
    {
      citem.setScore(score = scoreContItem(citem));
      totalScore += score;
    }
    
    // Normalize
    for (ContextualItem citem : context.getContext())
    { citem.setScore(citem.getScore() / totalScore);
    }
  }
  
  public boolean smallerScoresAreBetter()
  { return true;
  }
  
  protected double m_srcTokCount;
  protected double m_trgTokCount;  
}
