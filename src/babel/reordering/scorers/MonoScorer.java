package babel.reordering.scorers;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.properties.order.PhraseContext;

public class MonoScorer extends ReorderingScorer {

  protected static final Log LOG = LogFactory.getLog(MonoScorer.class);
  
  public MonoScorer(PhraseTable phraseTable) {
    m_phraseTable = phraseTable;
  }
  
  public OrderTriple scoreBefore(Phrase srcPhrase, Phrase trgPhrase) {

    PhraseContext srcPhraseContext = (PhraseContext)srcPhrase.getProperty(PhraseContext.class.getName());
    PhraseContext trgPhraseContext = (PhraseContext)trgPhrase.getProperty(PhraseContext.class.getName());

    if (srcPhraseContext == null || trgPhraseContext == null) {
      return null;
    }
    
    double weight;
    int logCount = 0;
    double numMono, numSwap, numDiscont;
    
    Map<Phrase, Integer> beforeSrcPhrases = srcPhraseContext.getBefore();        
    double numBeforeMono = 0, numBeforeSwap = 0, numBeforeOutOfOrder = 0;
    int count = 0;
    
    for (Phrase beforeSrcPhrase : beforeSrcPhrases.keySet()) {
      
      if (count++ >= 1000) {
        break;
      }
      
      for (Phrase transTrgPhrase : m_phraseTable.getTrgPhrases(beforeSrcPhrase)) {
        //count++;
        if (trgPhraseContext.hasAnywhere(transTrgPhrase)) {
          
          weight = 1.0;
          //weight = m_phraseTable.getProps(beforeSrcPhrase, transTrgPhrase).getPairFeatVal(PairFeat.EF); // PairFeat.EF  
                
          numBeforeMono += weight * (numMono = trgPhraseContext.beforeCount(transTrgPhrase));
          numBeforeSwap += weight * (numSwap = trgPhraseContext.afterCount(transTrgPhrase));
          numBeforeOutOfOrder += weight * (numDiscont = trgPhraseContext.outOfOrderCount(transTrgPhrase));
        
          if (logCount > 0) {
            
            StringBuilder strBldLog = new StringBuilder();
                  
            if (numMono > 0) {
              strBldLog.append("Mono (" +  numMono + ") : ");
            } else if (numSwap > 0) {
              strBldLog.append("Swap (" +  numSwap + ") : ");
            } else {
              strBldLog.append("Discontinuous (" +  numDiscont + ") : ");
            }
                  
            strBldLog.append(" phrase pair: (" + srcPhrase.toString() + "|" + trgPhrase.toString() + ")");
            strBldLog.append(", context phrase translations: (" + beforeSrcPhrase.toString() + "->" + transTrgPhrase.toString() + ")");
            strBldLog.append(", phrase table weight: " + weight);
            LOG.info(strBldLog.toString());
                    
            logCount--;                  
          }
        }
      }
    }

    double totalBefore = numBeforeMono + numBeforeSwap + numBeforeOutOfOrder;
    return (totalBefore == 0) ? null : new OrderTriple(numBeforeMono / totalBefore, numBeforeSwap / totalBefore, numBeforeOutOfOrder / totalBefore);
  }

  public OrderTriple scoreAfter(Phrase srcPhrase, Phrase trgPhrase) {
    
    PhraseContext srcPhraseContext = (PhraseContext)srcPhrase.getProperty(PhraseContext.class.getName());
    PhraseContext trgPhraseContext = (PhraseContext)trgPhrase.getProperty(PhraseContext.class.getName());
    
    if (srcPhraseContext == null || trgPhraseContext == null) {
      return null;
    }
    
    double weight;
    int logCount = 0;
    double numMono, numSwap, numDiscont;
    
    Map<Phrase, Integer> afterSrcPhrases = srcPhraseContext.getAfter();
    double numAfterMono = 0, numAfterSwap = 0, numAfterOutOfOrder = 0;
    int count = 0;
    
    for (Phrase afterSrcPhrase : afterSrcPhrases.keySet()) {

      if (count++ >= 1000) {
        break;
      }
      
      for (Phrase transTrgPhrase : m_phraseTable.getTrgPhrases(afterSrcPhrase)) {
        if (trgPhraseContext.hasAnywhere(transTrgPhrase)) {

          weight = 1.0;
          //weight = m_phraseTable.getProps(afterSrcPhrase, transTrgPhrase).getPairFeatVal(PairFeat.EF); // PairFeat.EF
                
          numAfterMono += weight * (numMono = trgPhraseContext.afterCount(transTrgPhrase));
          numAfterSwap += weight * (numSwap = trgPhraseContext.beforeCount(transTrgPhrase));
          numAfterOutOfOrder += weight * (numDiscont = trgPhraseContext.outOfOrderCount(transTrgPhrase));

          if (logCount > 0) {
                  
            StringBuilder strBldLog = new StringBuilder();
                  
            if (numMono > 0) {
              strBldLog.append("Mono (" +  numMono + ") : ");
            } else if (numSwap > 0) {
              strBldLog.append("Swap (" +  numSwap + ") : ");
            } else {
              strBldLog.append("Discontinuous (" +  numDiscont + ") : ");
            }
                  
            strBldLog.append(" phrase pair: (" + srcPhrase.toString() + "|" + trgPhrase.toString() + ")");
            strBldLog.append(", context phrase translations: (" + afterSrcPhrase.toString() + "->" + transTrgPhrase.toString() + ")");
            strBldLog.append(", phrase table weight: " + weight);
            LOG.info(strBldLog.toString());
                    
            logCount--;
          }                  
        }
      }
    }
    
    double totalAfter = numAfterMono + numAfterSwap + numAfterOutOfOrder;
    return (totalAfter == 0) ? null : new OrderTriple(numAfterMono / totalAfter, numAfterSwap / totalAfter, numAfterOutOfOrder / totalAfter);
  }
  
  protected PhraseTable m_phraseTable;
}
