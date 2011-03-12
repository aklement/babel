package babel.content.eqclasses.properties.lshcontext;

import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.LSHPropertyCollector;
import babel.content.eqclasses.properties.context.Context;
import babel.content.eqclasses.properties.context.Context.ContextualItem;
import babel.util.jerboa.LSH;

/** 
 * Generates LSH context signatures from existing Context properties.  Assumes
 * that the context is projected / scored.
 */
public class LSHContextCollector extends LSHPropertyCollector {

  private static final Log LOG = LogFactory.getLog(LSHContextCollector.class);
  private static final LSH CONTEXT_LSH = new LSH();
  
  public LSHContextCollector(boolean removeOrigProp) {
    super(removeOrigProp);
  }
  
  public void collectProperty(Set<? extends EquivalenceClass> eqClasses) throws Exception {

    Context context;
    HashMap<String, Double> features = new HashMap<String, Double>();
    int numUnscored = 0;
    
    for (EquivalenceClass eq : eqClasses)
    {
      // Get its context prop
      context = (Context)eq.getProperty(Context.class.getName());
      
      if (context == null || !context.areContItemsScored()) {
        numUnscored++;
      } else {
     
        features.clear();
        
        for (ContextualItem contItem : context.getContextualItems()) {
          features.put(contItem.getContextEqId().toString(), contItem.getScore());
        }
        
        eq.setProperty(new LSHContext(eq, CONTEXT_LSH.buildSignature(features)));
        if (m_removeOrigProp) {
          eq.removeProperty(Context.class.getName());
        }
      }
    }
    
    if (numUnscored > 0) {
      LOG.error("Could not compute LSH signature for " + numUnscored + " equvalence classes (either missing or unscored context).");
    }
  }
}
