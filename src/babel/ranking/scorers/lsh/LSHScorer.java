package babel.ranking.scorers.lsh;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.LSHProperty;
import babel.ranking.scorers.Scorer;
import babel.util.jerboa.LSH;

public class LSHScorer extends Scorer {
  
  protected static final Log LOG = LogFactory.getLog(LSHScorer.class);

  public LSHScorer(Class<? extends LSHProperty> propClass) {
    m_propClass = propClass;
  }
  
  public double score(EquivalenceClass srcEq, EquivalenceClass trgEq) {
    
    LSHProperty srcProp = (LSHProperty)srcEq.getProperty(m_propClass.getName());     
    LSHProperty trgProp = (LSHProperty)trgEq.getProperty(m_propClass.getName()); 

    if (srcProp == null || trgProp == null) {
      throw new IllegalArgumentException("At least one of the classes has no property " + m_propClass.getName() + ".");
    }
    
    return Math.max(0.0, LSH.scoreSignatures(srcProp.getSignature(), trgProp.getSignature()));
  }

  public void prepare(EquivalenceClass eq) {
  }
  
  public boolean smallerScoresAreBetter() {
    return false;
  }
 
  protected Class<? extends LSHProperty> m_propClass;
}