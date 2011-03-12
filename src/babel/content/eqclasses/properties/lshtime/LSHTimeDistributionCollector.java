package babel.content.eqclasses.properties.lshtime;

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.LSHPropertyCollector;
import babel.content.eqclasses.properties.time.TimeDistribution;
import babel.util.jerboa.LSH;

/** 
 * Generates LSH context signatures from existing Context properties.  Assumes
 * that the context is projected / scored.
 */
public class LSHTimeDistributionCollector extends LSHPropertyCollector {

  private static final Log LOG = LogFactory.getLog(LSHTimeDistributionCollector.class);
  private static final LSH TIME_LSH = new LSH();
  
  public LSHTimeDistributionCollector(boolean removeOrigProp) {
    super(removeOrigProp);
  }
  
  public void collectProperty(Set<? extends EquivalenceClass> eqClasses) throws Exception {

    TimeDistribution distro;
    HashMap<Integer, Double> bins;
    HashMap<String, Double> features = new HashMap<String, Double>();
    int numUnscored = 0;
    
    for (EquivalenceClass eq : eqClasses)
    {
      // Get its context prop
      distro = (TimeDistribution)eq.getProperty(TimeDistribution.class.getName());
            
      if (distro == null || !distro.isNormalized()) {
        numUnscored++;
      } else {
     
        features.clear();
        bins = distro.getBins();
        
        for (Integer window : bins.keySet()) {
          features.put(window.toString(), bins.get(window));
        }
        
        eq.setProperty(new LSHTimeDistribution(eq, TIME_LSH.buildSignature(features)));
        
        if (m_removeOrigProp) {
          eq.removeProperty(TimeDistribution.class.getName());
        }
      }
    }
    
    if (numUnscored > 0) {
      LOG.error("Could not compute LSH signature for " + numUnscored + " equvalence classes (either missing or unnormalized distribution).");
    }
  }
}