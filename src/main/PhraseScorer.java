package main;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.phrases.PhraseTable.PairProps;
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
    scorer.scorePhrases();
  }
  
  protected void scorePhrases() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outPhraseTable = Configurator.CONFIG.getString("output.PhraseTable");

    DataPreparer preparer = new DataPreparer();
    
    // Prepare equivalence classes
    preparer.preparePhrases();    
    
    PhraseTable phraseTable = preparer.getPhraseTable();
    Set<Phrase> srcPhrases = phraseTable.getAllSrcPhrases();
    Set<Phrase> trgPhrases = phraseTable.getAllTrgPhrases();
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
        
    // Pre-process properties (i.e. project contexts, normalizes distributions)
    preparer.prepareProperties(true, srcPhrases, contextScorer, timeScorer);
    preparer.prepareProperties(false, trgPhrases, contextScorer, timeScorer);
    
    PairProps props;
    
    for (Phrase srcPhrase : srcPhrases) {
      for (Phrase trgPhrase : phraseTable.getTrgPhrases(srcPhrase)) {
        props = phraseTable.getProps(srcPhrase, trgPhrase);
        
        props.addFeatureVal(contextScorer.score(srcPhrase, trgPhrase));
        props.addFeatureVal(timeScorer.score(srcPhrase, trgPhrase));
        props.addFeatureVal(scoreEdit(srcPhrase, trgPhrase, props));
      }
    }
    
    phraseTable.savePhraseTable(outDir + "/" + outPhraseTable);
    
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
}
