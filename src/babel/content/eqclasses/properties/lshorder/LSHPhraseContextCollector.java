package babel.content.eqclasses.properties.lshorder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.properties.LSHPropertyCollector;
import babel.content.eqclasses.properties.order.PhraseContext;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;

import babel.util.jerboa.LSH;

/** 
 * Generates LSH phrase context signature triples from existing PhraseContext
 * properties. Projects the phrase context vectors using the phrase table.
 */
public class LSHPhraseContextCollector extends LSHPropertyCollector {
  
  private static final Log LOG = LogFactory.getLog(LSHPhraseContextCollector.class);
  private static final LSH PHRASE_CONTEXT_LSH = new LSH();
  private static final byte[] EMPTY_SIG = PHRASE_CONTEXT_LSH.buildSignature(new HashMap<String, Double>());
  
  public LSHPhraseContextCollector(boolean removeOrigProp, PhraseTable phTable) {
    super(removeOrigProp);
    m_phraseDict = phTable;
    m_rand = new Random();
  }
  
  public void collectProperty(Set<? extends EquivalenceClass> eqClasses) throws Exception {

    PhraseContext phContext;
    EqType type;
    boolean src;
    int noPropCount = 0;
        
    for (EquivalenceClass eq : eqClasses)
    {
      // Get its PhraseContext and type properties
      phContext = (PhraseContext)eq.getProperty(PhraseContext.class.getName());
      type = ((Type)eq.getProperty(Type.class.getName())).getType();
      
      if (EqType.NONE.equals(type)) {
        LOG.error("Cannot collect phrase context for a phrase without the Type property");
        throw new Exception("Cannot collect phrase context for a phrase without the Type property");
      } else {
        src = EqType.SOURCE.equals(type);
      }
      
      if (phContext != null) {
        eq.setProperty(new LSHPhraseContext(eq, getSignature(src, phContext.getBefore()), getSignature(src, phContext.getAfter()), getSignature(src, phContext.getDiscontinuous())));
        if (m_removeOrigProp) {
          eq.removeProperty(PhraseContext.class.getName());
        }
      } else {
        eq.setProperty(new LSHPhraseContext(eq, EMPTY_SIG, EMPTY_SIG, EMPTY_SIG));
        noPropCount++;
      }
    }
    
    LOG.info(" - " + noPropCount + " of " + eqClasses.size() + " phrases did not have PhraseContext property");
  }
  
  protected byte[] getSignature(boolean src, Map<Phrase, Integer> phContext) {
    
    HashMap<String, Double> phCounts = new HashMap<String, Double>();
    int count;
    
    for (Phrase contPhrase : phContext.keySet()) {
      count = phContext.get(contPhrase);
      
      if (src) {
        // Project using the phrase table translations. TODO: Incorrect, in principle: should not add all translations.
        
        // Map to all
        for (Phrase contPhraseTrans : m_phraseDict.getTrgPhrases(contPhrase)) {
          phCounts.put(Long.toString(contPhraseTrans.getId()), (double)count);          
        }
        
        // Map to one random
        //Phrase p = getRandomPhrase(m_phraseDict.getTrgPhrases(contPhrase));
        //if (p != null) {
        //  phCounts.put(Long.toString(p.getId()), (double)count);          
        //}

      } else {
        phCounts.put(Long.toString(contPhrase.getId()), (double)count);        
      }
    }
    
    return PHRASE_CONTEXT_LSH.buildSignature(phCounts);
  }
  
  protected Phrase getRandomPhrase(Set<Phrase> phrases) {
    
    Phrase p = null;
    
    if (!phrases.isEmpty()) {
      ArrayList<Phrase> phraseList = new ArrayList<Phrase>(phrases);
      p = phraseList.get(m_rand.nextInt(phraseList.size()));
    }
      
    return p;
  }
  
  protected PhraseTable m_phraseDict;
  protected Random m_rand;
}