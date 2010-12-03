package main.phrases;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhraseTable;

import babel.ranking.scorers.Scorer;
import babel.ranking.scorers.context.DictScorer;
import babel.ranking.scorers.context.FungS1Scorer;
import babel.ranking.scorers.timedistribution.TimeDistributionCosineScorer;

import babel.util.config.Configurator;
import babel.util.dict.SimpleDictionary;

public class PhraseScorer
{
  protected static final Log LOG = LogFactory.getLog(PhraseScorer.class);
  protected static final int PHRASE_TABLE_CHUNK = Integer.MAX_VALUE;
  
  public static void main(String[] args) throws Exception {
    
    LOG.info("\n" + Configurator.getConfigDescriptor());
 
    boolean doMonoFeatures = Configurator.CONFIG.getBoolean("preprocessing.phrases.DoMonoFeatures");
    boolean doReordering = Configurator.CONFIG.getBoolean("preprocessing.phrases.DoReordering");
    
    PhraseScorer scorer = new PhraseScorer();
    
    if (doMonoFeatures && doReordering) {
      LOG.info("===> Estimating both monolingual and reordering features <===");
      scorer.scorePhraseFeaturesAndOrder();
    } else if (doMonoFeatures) {
      LOG.info("===> Estimating  monolingual features only <===");
      scorer.scorePhraseFeaturesOnly();
    } else if (doReordering) {
      LOG.info("===> Estimating reordering features only <===");
      scorer.scorePhraseOrderOnly();
    } else {
      LOG.warn("===> NOT estimating either monolingual or reordering features <===");
    }
    
    LOG.info("===> Done <===");
  }

  protected void scorePhraseOrderOnly() throws Exception
  {
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outMonoReorderingTable = Configurator.CONFIG.getString("output.MonoReorderingTable");
    int numReorderingThreads = Configurator.CONFIG.getInt("preprocessing.phrases.ReorderingThreads");
    
    PhrasePreparer preparer = new PhrasePreparer();
    preparer.preparePhrasesForOrderingOnly();
    
    PhraseTable phraseTable = preparer.getPhraseTable();
    
    LOG.info("--- Estimating reordering features ---");
    
    // Estimate reordering features
    (new OrderEstimator(phraseTable, numReorderingThreads, preparer.getMaxTrgPhrCount())).estimateReordering();
    
    // Save the new phrase table (containing mono features)
    phraseTable.saveReorderingTable(outDir + "/" + outMonoReorderingTable);    
  }
  
  protected void scorePhraseFeaturesOnly() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outMonoPhraseTable = Configurator.CONFIG.getString("output.MonoPhraseTable");
    String outAddMonoPhraseTable = Configurator.CONFIG.getString("output.AddMonoPhraseTable");
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.MonoScoringThreads");

    if (outMonoPhraseTable != null) {
      outMonoPhraseTable = outDir + "/" + outMonoPhraseTable; 
    }
    
    if (outAddMonoPhraseTable != null) {
      outAddMonoPhraseTable = outDir + "/" + outAddMonoPhraseTable; 
    }
    
    LOG.info("--- Preparing for estimating monolingual features ---");    

    PhraseTable phraseTableChunk;
    int chunkNum = 0;

    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForChunkFeaturesCollection();
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    SimpleDictionary translitDict = preparer.getTranslitDict();
    
    LOG.info("--- Estimating monolingual features for phrase table chunks ---");    
    
    // Split up the phrase table and process one chunk at a time
    while (preparer.preparePhraseChunkForFeaturesOnly(chunkNum++, PHRASE_TABLE_CHUNK) > 0) {

      phraseTableChunk = preparer.getPhraseTable();
      
      LOG.info(" - Estimating monolingual features for phrase table chunk " + (chunkNum-1));    
    
      // Pre-process properties (i.e. project contexts, normalizes distributions)
      preparer.prepareProperties(true, phraseTableChunk.getAllSrcPhrases(), contextScorer, timeScorer);
      preparer.prepareProperties(false, phraseTableChunk.getAllTrgPhrases(), contextScorer, timeScorer);
    
      // Estimate monolingual similarity features
      (new FeatureEstimator(phraseTableChunk, numMonoScoringThreads, contextScorer, timeScorer, translitDict)).estimateFeatures();
    
      // Save the new phrase table (containing mono features)
      phraseTableChunk.savePhraseTable(outMonoPhraseTable, outAddMonoPhraseTable);
    }    
  }
  
  protected void scorePhraseFeaturesAndOrder() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outMonoPhraseTable = Configurator.CONFIG.getString("output.MonoPhraseTable");
    String outAddMonoPhraseTable = Configurator.CONFIG.getString("output.AddMonoPhraseTable");
    String outMonoReorderingTable = Configurator.CONFIG.getString("output.MonoReorderingTable");
    int numReorderingThreads = Configurator.CONFIG.getInt("preprocessing.phrases.ReorderingThreads");
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.MonoScoringThreads");

    if (outMonoPhraseTable != null) {
      outMonoPhraseTable = outDir + "/" + outMonoPhraseTable; 
    }
    
    if (outAddMonoPhraseTable != null) {
      outAddMonoPhraseTable = outDir + "/" + outAddMonoPhraseTable; 
    }
    
    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.preparePhrasesForFeaturesAndOrder();
    
    PhraseTable phraseTable = preparer.getPhraseTable();    
    Set<Phrase> srcPhrases = phraseTable.getAllSrcPhrases();
    Set<Phrase> trgPhrases = phraseTable.getAllTrgPhrases();
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    
    SimpleDictionary translitDict = preparer.getTranslitDict();
    
    LOG.info("--- Estimating monolingual features ---");
    
    // Pre-process properties (i.e. project contexts, normalizes distributions)
    preparer.prepareProperties(true, srcPhrases, contextScorer, timeScorer);
    preparer.prepareProperties(false, trgPhrases, contextScorer, timeScorer);
    
    // Estimate monolingual similarity features
    (new FeatureEstimator(phraseTable, numMonoScoringThreads, contextScorer, timeScorer, translitDict)).estimateFeatures();
    
    // Save the new phrase table (containing mono features)
    phraseTable.savePhraseTable(outMonoPhraseTable, outAddMonoPhraseTable);
    
    LOG.info("--- Estimating reordering features ---");
    
    // Estimate reordering features
    (new OrderEstimator(phraseTable, numReorderingThreads, preparer.getMaxTrgPhrCount())).estimateReordering();
    
    // Save the reordering table
    phraseTable.saveReorderingTable(outDir + "/" + outMonoReorderingTable);    
  }
}
