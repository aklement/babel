package main;

import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.phrases.PhraseTable.PairFeat;
import babel.content.eqclasses.phrases.PhraseTable.PairProps;
import babel.content.eqclasses.properties.PhraseContext;
import babel.ranking.scorers.Scorer;
import babel.ranking.scorers.context.DictScorer;
import babel.ranking.scorers.context.FungS1Scorer;
import babel.ranking.scorers.timedistribution.TimeDistributionCosineScorer;
import babel.util.config.Configurator;
import babel.util.misc.EditDistance;

public class PhraseScorer
{
  public static final Log LOG = LogFactory.getLog(PhraseScorer.class);
  
  public static void main(String[] args) throws Exception {
    
    LOG.info("\n" + Configurator.getConfigDescriptor());
 
    PhraseScorer scorer = new PhraseScorer();   
    scorer.scorePhrasesOrder();
    // TODO: Faster uncomment line below, remove line above
    //scorer.scorePhrases();
  }

  // TODO: Faster Used scorePhrasesOrder() instead of scorePhrases()
  protected void scorePhrasesOrder() throws Exception
  {
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outReorderingTable = Configurator.CONFIG.getString("output.ReorderingTable");
    
    DataPreparer preparer = new DataPreparer();
    
    // Prepare equivalence classes
    preparer.preparePhrasesForOrderingOnly();

    PhraseTable phraseTable = preparer.getPhraseTable();
    
    LOG.info("--- Estimating reordering features ---");
    
    // Estimate reordering features
    estimateReordering(phraseTable);
    
    // Save the new phrase table (containing mono features) and the reordering table
    phraseTable.saveReorderingTable(outDir + "/" + outReorderingTable);
    
    LOG.info("--- Done ---");
  }

  protected void scorePhrases() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outPhraseTable = Configurator.CONFIG.getString("output.PhraseTable");
    String outReorderingTable = Configurator.CONFIG.getString("output.ReorderingTable");

    DataPreparer preparer = new DataPreparer();
    
    // Prepare equivalence classes
    preparer.preparePhrases();
    
    PhraseTable phraseTable = preparer.getPhraseTable();    
    Set<Phrase> srcPhrases = phraseTable.getAllSrcPhrases();
    Set<Phrase> trgPhrases = phraseTable.getAllTrgPhrases();
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    
    LOG.info("--- Estimating monolingual features ---");
    
    // Pre-process properties (i.e. project contexts, normalizes distributions)
    preparer.prepareProperties(true, srcPhrases, contextScorer, timeScorer);
    preparer.prepareProperties(false, trgPhrases, contextScorer, timeScorer);
    
    PairProps props;
    
    for (Phrase srcPhrase : srcPhrases) {
      for (Phrase trgPhrase : phraseTable.getTrgPhrases(srcPhrase)) {
        props = phraseTable.getProps(srcPhrase, trgPhrase);
        
        props.addPairFeatVal(contextScorer.score(srcPhrase, trgPhrase));
        props.addPairFeatVal(timeScorer.score(srcPhrase, trgPhrase));
        props.addPairFeatVal(scoreEdit(srcPhrase, trgPhrase, props));
      }
    }
    
    LOG.info("--- Estimating reordering features ---");
    
    // Estimate reordering features
    estimateReordering(phraseTable);
    
    // Save the new phrase table (containing mono features) and the reordering table
    phraseTable.savePhraseTable(outDir + "/" + outPhraseTable);
    phraseTable.saveReorderingTable(outDir + "/" + outReorderingTable);
    
    LOG.info("--- Done ---");
  }
  
  // Compute average per charachted forward and backward edit distance
  protected double scoreEdit(Phrase srcPhrase, Phrase trgPhrase, PairProps props) {
    String[] srcWords = srcPhrase.getWord().split(" ");
    String[] trgWords = trgPhrase.getWord().split(" ");
    
    double letterCount = 0;
    double numEdits = 0; 
    
    // Forward counts
    int[][] aligns = props.getForwardAligns();
    for (int i = 0; i < aligns.length; i++) {
      if (aligns[i] != null) {
        for (int j = 0; j < aligns[i].length; j++) {
          numEdits += EditDistance.distance(srcWords[i], trgWords[aligns[i][j]]);
          letterCount += (double)(srcWords[i].length() + trgWords[aligns[i][j]].length()) / 2.0;
        }
      }
    }
    
    // Backward counts
    aligns = props.getBackwardAligns();
    for (int i = 0; i < aligns.length; i++) {
      if (aligns[i] != null) {
        for (int j = 0; j < aligns[i].length; j++) {
          numEdits += EditDistance.distance(trgWords[i], srcWords[aligns[i][j]]);
          letterCount += (double)(trgWords[i].length() + srcWords[aligns[i][j]].length()) / 2.0;
        }
      }
    }
    
    return numEdits / letterCount;
  }
  
  protected void estimateReordering(PhraseTable phraseTable) {
    
    Set<Phrase> srcPhrases = phraseTable.getAllSrcPhrases();
    
    PhraseContext srcPhraseContext, trgPhraseContext;
    Map<Phrase, Integer> beforeSrcPhrases, afterSrcPhrases;
    double numBeforeMono, numBeforeSwap, numBeforeOutOfOrder;
    double numAfterMono, numAfterSwap, numAfterOutOfOrder;
    double total, weight;
    int logCount = 0;
    int logMono, logSwap, logNoOrder;
    StringBuilder strBldLog;

    for (Phrase srcPhrase : srcPhrases) {
      
      if (null != (srcPhraseContext = (PhraseContext)srcPhrase.getProperty(PhraseContext.class.getName()))) {
      
        beforeSrcPhrases = srcPhraseContext.getBefore();
        afterSrcPhrases = srcPhraseContext.getAfter();
        
        for (Phrase trgPhrase : phraseTable.getTrgPhrases(srcPhrase)) {
        
          if (null != (trgPhraseContext = (PhraseContext)trgPhrase.getProperty(PhraseContext.class.getName()))) {
            
            PairProps props = phraseTable.getProps(srcPhrase, trgPhrase);

            numBeforeMono = numBeforeSwap = numBeforeOutOfOrder = 0;

            for (Phrase beforeSrcPhrase : beforeSrcPhrases.keySet()) {
              
              for (Phrase transTrgPhrase : phraseTable.getTrgPhrases(beforeSrcPhrase)) {
               
                // TODO: Not using source counts
                weight = phraseTable.getProps(beforeSrcPhrase, transTrgPhrase).getPairFeatVal(PairFeat.EF);
                //weight = 1.0;
                
                numBeforeMono += weight * (logMono = trgPhraseContext.beforeCount(transTrgPhrase));
                numBeforeSwap += weight * (logSwap = trgPhraseContext.afterCount(transTrgPhrase));
                numBeforeOutOfOrder += weight * (logNoOrder = trgPhraseContext.outOfOrderCount(transTrgPhrase));
                
                if (logCount > 0) {
                
                  strBldLog = new StringBuilder();
                  
                  if (logMono > 0) {
                    strBldLog.append("Mono (" +  logMono + ") : ");
                  } else if (logSwap > 0) {
                    strBldLog.append("Swap (" +  logSwap + ") : ");
                  } else if (logNoOrder > 0) {
                    strBldLog.append("Out of order (" +  logNoOrder + ") : ");
                  }
                  
                  if (logMono > 0 || logSwap > 0 || logNoOrder > 0) {
                    strBldLog.append(" phrase pair: (" + srcPhrase.toString() + "|" + trgPhrase.toString() + ")");
                    strBldLog.append(", context phrase translations: (" + beforeSrcPhrase.toString() + "->" + transTrgPhrase.toString() + ")");
                    strBldLog.append(", phrase table weight: " + weight);
                    LOG.info(strBldLog.toString());
                  }
                  
                  logCount--;
                }
                
              }
            }
            
            if (0 != (total = numBeforeMono + numBeforeSwap + numBeforeOutOfOrder)) {
              props.setBeforeOrderFeatVals(numBeforeMono / total, numBeforeSwap / total, numBeforeOutOfOrder / total);
            }

            numAfterMono = numAfterSwap = numAfterOutOfOrder = 0;

            for (Phrase afterSrcPhrase : afterSrcPhrases.keySet()) {
              
              for (Phrase transTrgPhrase : phraseTable.getTrgPhrases(afterSrcPhrase)) {
               
                weight = phraseTable.getProps(afterSrcPhrase, transTrgPhrase).getPairFeatVal(PairFeat.EF);
                //weight = 1.0;
                
                numAfterMono += weight * trgPhraseContext.afterCount(transTrgPhrase);
                numAfterSwap += weight * trgPhraseContext.beforeCount(transTrgPhrase);
                numAfterOutOfOrder += weight * trgPhraseContext.outOfOrderCount(transTrgPhrase);
              }
            }

            if (0 != (total = numAfterMono + numAfterSwap + numAfterOutOfOrder)) {
              props.setAfterOrderFeatVals(numAfterMono / total, numAfterSwap / total, numAfterOutOfOrder / total);
            }
          }
        }
      }
    }
  }
}
