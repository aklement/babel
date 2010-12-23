package main.phrases;

import java.util.HashSet;
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
  protected static final int KEEP_PH_CONTEXT = 1000;
  
  
  public static void main(String[] args) throws Exception {
    
    LOG.info("\n" + Configurator.getConfigDescriptor());
 
    boolean doMonoFeatures = Configurator.CONFIG.getBoolean("preprocessing.phrases.DoMonoFeatures");
    boolean doReordering = Configurator.CONFIG.getBoolean("preprocessing.phrases.DoReordering");
    
    PhraseScorer scorer = new PhraseScorer();
    
    if (doMonoFeatures && doReordering) {
      LOG.info("===> Estimating both monolingual and reordering features <===");
      scorer.scorePhraseFeaturesAndOrder();
    } else if (doMonoFeatures) {
      //LOG.info("===> Estimating  monolingual features only (for Anni) <===");
      //scorer.scorePhraseFeaturesOnlyForAnni();
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
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? 
        Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;
    
    LOG.info("--- Preparing for estimating reordering features " + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    PhrasePreparer preparer = new PhrasePreparer();
    preparer.prepareForChunkOrderCollection();
    PhraseTable phraseTable = preparer.getPhraseTable();
    int chunkNum = 0;
    Set<Phrase> chunk, trgChunk;
    
    OrderEstimator orderEstimator = new OrderEstimator(phraseTable, numReorderingThreads, preparer.getMaxTrgPhrCount());
        
    // Split up the phrase table and process one chunk at a time
    while ((chunk = preparer.getNextChunk(chunkSize)) != null) {

      LOG.info(" - Preparing chunk " + (chunkNum++) + " of phrase table ...");
      trgChunk = phraseTable.getTrgPhrases(chunk);
      
      preparer.collectPropsForOrderOnly(chunk, trgChunk);
      preparer.pruneMostFrequentContext(true, chunk, KEEP_PH_CONTEXT, KEEP_PH_CONTEXT, 3 * KEEP_PH_CONTEXT);
      //preparer.pruneMostFrequentContext(false, trgChunk, KEEP_PH_CONTEXT, KEEP_PH_CONTEXT, 3 * KEEP_PH_CONTEXT);
         
      LOG.info(" - Estimating reordering features for phrase table chunk " + (chunkNum-1) + "...");

      // Estimate reordering features
      orderEstimator.estimateReordering(chunk);
      
      // Save the new phrase table (containing mono features)
      phraseTable.saveReorderingTableChunk(chunk, outDir + "/" + outMonoReorderingTable);
      
      // Clear the collected reordering features
      preparer.clearReorderingFeatures(chunk);
      preparer.clearReorderingFeatures(phraseTable.getTrgPhrases(chunk));
    }
  }
  
  protected void scorePhraseFeaturesOnly() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outMonoPhraseTable = Configurator.CONFIG.getString("output.MonoPhraseTable");
    String outAddMonoPhraseTable = Configurator.CONFIG.getString("output.AddMonoPhraseTable");
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.MonoScoringThreads");
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? 
        Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;    
    boolean useAlignments = Configurator.CONFIG.getBoolean("preprocessing.phrases.UseAlignmentsForMonoScores");

    if (outMonoPhraseTable != null) {
      outMonoPhraseTable = outDir + "/" + outMonoPhraseTable; 
    }
    
    if (outAddMonoPhraseTable != null) {
      outAddMonoPhraseTable = outDir + "/" + outAddMonoPhraseTable; 
    }
    
    LOG.info("--- Preparing for estimating monolingual features " + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForChunkFeaturesCollection();
    PhraseTable phraseTable = preparer.getPhraseTable();
    int chunkNum = 0;
    Set<Phrase> chunk, srcChunkToProcess, trgChunkToProcess;
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    SimpleDictionary translitDict = preparer.getTranslitDict();
    Set<Phrase> singleTokenSrcPhrases = phraseTable.getAllSingleTokenSrcPhrases();
    Set<Phrase> singleTokenTrgPhrases = phraseTable.getAllSingleTokenTrgPhrases();
    FeatureEstimator featEstimator = new FeatureEstimator(phraseTable, singleTokenSrcPhrases, singleTokenTrgPhrases, numMonoScoringThreads, contextScorer, timeScorer, translitDict, useAlignments);

    // Prepare for single tokens first (we are going to need them when estimating for longer phrases)
    preparer.collectPropsForFeaturesOnly(singleTokenSrcPhrases, singleTokenTrgPhrases);
    // Pre-process properties (i.e. project contexts, normalizes distributions)
    preparer.prepareProperties(true, singleTokenSrcPhrases, contextScorer, timeScorer);
    preparer.prepareProperties(false, singleTokenTrgPhrases, contextScorer, timeScorer);
    
    // Split up the phrase table and process one chunk at a time
    while ((chunk = preparer.getNextChunk(chunkSize)) != null) {
      
      LOG.info(" - Preparing chunk " + (chunkNum++) + " of phrase table ...");
      
      (srcChunkToProcess = new HashSet<Phrase>(chunk)).removeAll(singleTokenSrcPhrases);
      (trgChunkToProcess = new HashSet<Phrase>(phraseTable.getTrgPhrases(chunk))).removeAll(singleTokenTrgPhrases);
    
      preparer.collectPropsForFeaturesOnly(srcChunkToProcess, trgChunkToProcess);
      // Pre-process properties (i.e. project contexts, normalizes distributions)
      preparer.prepareProperties(true, srcChunkToProcess, contextScorer, timeScorer);
      preparer.prepareProperties(false, trgChunkToProcess, contextScorer, timeScorer);
      
      LOG.info(" - Estimating monolingual features for phrase table chunk " + (chunkNum-1) + "...");
      
      // Estimate monolingual similarity features
      featEstimator.estimateFeatures(chunk);
    
      // Save the new phrase table (containing mono features)
      phraseTable.savePhraseTableChunk(chunk, outMonoPhraseTable, outAddMonoPhraseTable, useAlignments);
      
      // Clear collected phrase features
      preparer.clearPhraseTableFeatures(srcChunkToProcess);
      preparer.clearPhraseTableFeatures(trgChunkToProcess);
    }    
  }

  protected void scorePhraseFeaturesOnlyForAnni() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outMonoPhraseTable = Configurator.CONFIG.getString("output.MonoPhraseTable");
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.MonoScoringThreads");
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? 
        Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;
        
    LOG.info("--- Preparing for estimating monolingual features " + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForChunkFeaturesCollectionForAnni(chunkSize);
    int chunkNum = 0;
    Set<Phrase> srcChunkToProcess, trgChunkToProcess;
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    SimpleDictionary translitDict = preparer.getTranslitDict();
    
    // Split up the phrase table and process one chunk at a time
    while (preparer.readNextChunkForAnni(chunkSize, chunkNum++) > 0) {
            
      srcChunkToProcess = preparer.getPhraseTable().getAllSrcPhrases();
      trgChunkToProcess = preparer.getPhraseTable().getAllTrgPhrases();
    
      preparer.collectPropsForFeaturesOnly(srcChunkToProcess, trgChunkToProcess);
      preparer.prepareProperties(true, srcChunkToProcess, contextScorer, timeScorer);
      preparer.prepareProperties(false, trgChunkToProcess, contextScorer, timeScorer);
      
      LOG.info(" - Estimating monolingual features for phrase table chunk " + (chunkNum-1) + "...");
      
      (new FeatureEstimator(preparer.getPhraseTable(), numMonoScoringThreads, contextScorer, timeScorer, translitDict, false)).estimateFeatures(srcChunkToProcess);
    
      // Save the new phrase table (containing mono features)
      preparer.getPhraseTable().savePhraseTableChunk(srcChunkToProcess, outDir + "/" + outMonoPhraseTable, null, false);      
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
    boolean useAlignments = Configurator.CONFIG.getBoolean("preprocessing.phrases.UseAlignmentsForMonoScores");

    if (outMonoPhraseTable != null) {
      outMonoPhraseTable = outDir + "/" + outMonoPhraseTable; 
    }
    
    if (outAddMonoPhraseTable != null) {
      outAddMonoPhraseTable = outDir + "/" + outAddMonoPhraseTable; 
    }
    
    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForFeaturesAndOrderCollection();
    
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
    (new FeatureEstimator(phraseTable, numMonoScoringThreads, contextScorer, timeScorer, translitDict, useAlignments)).estimateFeatures(srcPhrases);
    
    // Save the new phrase table (containing mono features)
    phraseTable.savePhraseTable(outMonoPhraseTable, outAddMonoPhraseTable, useAlignments);
    
    LOG.info("--- Estimating reordering features ---");
    preparer.pruneMostFrequentContext(true, srcPhrases, KEEP_PH_CONTEXT, KEEP_PH_CONTEXT, 3 * KEEP_PH_CONTEXT);
    //preparer.pruneMostFrequentContext(false, trgPhrases, KEEP_PH_CONTEXT, KEEP_PH_CONTEXT, 3 * KEEP_PH_CONTEXT);
    
    // Estimate reordering features
    (new OrderEstimator(phraseTable, numReorderingThreads, preparer.getMaxTrgPhrCount())).estimateReordering(srcPhrases);
    
    // Save the reordering table
    phraseTable.saveReorderingTable(outDir + "/" + outMonoReorderingTable);    
  }
}
