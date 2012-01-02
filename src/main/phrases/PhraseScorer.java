package main.phrases;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.phrases.PhraseTable.PairFeat;
import babel.content.eqclasses.properties.lshcontext.LSHContext;
import babel.content.eqclasses.properties.lshtime.LSHTimeDistribution;

import babel.ranking.scorers.Scorer;
import babel.ranking.scorers.context.DictScorer;
import babel.ranking.scorers.context.FungS1Scorer;
import babel.ranking.scorers.lsh.LSHScorer;
import babel.ranking.scorers.timedistribution.TimeDistributionCosineScorer;
import babel.reordering.scorers.LSHMonoScorer;
import babel.reordering.scorers.MonoScorer;

import babel.util.config.Configurator;
import babel.util.dict.SimpleDictionary;

public class PhraseScorer
{  
  protected static final Log LOG = LogFactory.getLog(PhraseScorer.class);
  protected static PairFeat[] FEATS_BPL = new PairFeat[]{PairFeat.FE, PairFeat.LEX_FE, PairFeat.EF, PairFeat.LEX_EF, PairFeat.PHPENALTY, PairFeat.PH_CONTEXT, PairFeat.PH_TIME, PairFeat.LEX_CONTEXT, PairFeat.LEX_TIME, PairFeat.LEX_EDIT};
  protected static PairFeat[] FEATS_PL = new PairFeat[]{PairFeat.PHPENALTY, PairFeat.PH_CONTEXT, PairFeat.PH_TIME, PairFeat.LEX_CONTEXT, PairFeat.LEX_TIME, PairFeat.LEX_EDIT};
  protected static PairFeat[] FEATS_P = new PairFeat[]{PairFeat.PHPENALTY, PairFeat.PH_CONTEXT, PairFeat.PH_TIME};
  protected static PairFeat[] FEATS_L = new PairFeat[]{PairFeat.PHPENALTY, PairFeat.LEX_CONTEXT, PairFeat.LEX_TIME, PairFeat.LEX_EDIT};
  protected static PairFeat[] FEATS_NONE = new PairFeat[]{PairFeat.PHPENALTY};

  public static void main(String[] args) throws Exception {
    
    LOG.info("\n" + Configurator.getConfigDescriptor());
 
    boolean doMonoFeatures = Configurator.CONFIG.getBoolean("preprocessing.phrases.features.DoMonoFeatures");
    boolean doReordering = Configurator.CONFIG.getBoolean("preprocessing.phrases.reordering.DoReordering");
    
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
    String outReorderingTableM = Configurator.CONFIG.getString("output.ReorderingTableM");
    int numReorderingThreads = Configurator.CONFIG.getInt("preprocessing.phrases.reordering.ReorderingThreads");
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;
    boolean approxOrder = Configurator.CONFIG.getBoolean("preprocessing.phrases.reordering.ApproxReordWithLSH");
    
    LOG.info("--- Preparing for estimating reordering features " + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    PhrasePreparer preparer = new PhrasePreparer();
    preparer.prepareForChunkOrderCollection();
    PhraseTable phraseTable = preparer.getPhraseTable();
    int chunkNum = 0;
    Set<Phrase> chunk, trgChunk;
    
    OrderEstimator orderEstimator = approxOrder ?
      new OrderEstimator(numReorderingThreads, new LSHMonoScorer(), phraseTable, preparer.getMaxTrgPhrCount()) :
      new OrderEstimator(numReorderingThreads, new MonoScorer(phraseTable), phraseTable, preparer.getMaxTrgPhrCount());

    // Split up the phrase table and process one chunk at a time
    while ((chunk = preparer.getNextChunk(chunkSize)) != null) {

      LOG.info(" - Preparing chunk " + (chunkNum++) + " of phrase table ...");
      trgChunk = phraseTable.getTrgPhrases(chunk);
      
      preparer.collectPropsForOrderOnly(chunk, trgChunk);
      preparer.pruneMostFrequentContext(true, chunk);
      preparer.pruneMostFrequentContext(false, trgChunk);

      // Pre-process ordering properties (for LSH)
      preparer.prepareOrderProps(true, chunk, approxOrder);
      preparer.prepareOrderProps(false, trgChunk, approxOrder);
      
      LOG.info(" - Estimating reordering features for phrase table chunk " + (chunkNum-1) + "...");

      // Estimate reordering features
      orderEstimator.estimateReordering(chunk);
      
      // Save the new phrase table (containing mono features)
      phraseTable.saveReorderingTableChunk(chunk, outDir + "/" + outReorderingTableM);
      
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
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.features.MonoScoringThreads");
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;    
    String outPhraseTableBPL = Configurator.CONFIG.getString("output.PhraseTableBPL");
    String outPhraseTablePL = Configurator.CONFIG.getString("output.PhraseTablePL");
    String outPhraseTableP = Configurator.CONFIG.getString("output.PhraseTableP");
    String outPhraseTableL = Configurator.CONFIG.getString("output.PhraseTableL");
    String outPhraseTableNone = Configurator.CONFIG.getString("output.PhraseTableNone");
    boolean collectPhraseFeats = (outPhraseTableBPL != null) || (outPhraseTablePL != null) || (outPhraseTableP != null);
    boolean collectLexFeats = (outPhraseTableBPL != null) || (outPhraseTablePL != null) || (outPhraseTableL != null);
    boolean approxFeats = Configurator.CONFIG.getBoolean("preprocessing.phrases.features.ApproxFeatsWithLSH");
      
    ///If Wikipedia-Temporal features, time window MUST be 1
    if ("wikitemp".equals(Configurator.CONFIG.getString("preprocessing.input.Time"))){
    	if (windowSize!=1){
    		windowSize=1;
    		LOG.info("WARNING: Using Wikipedia 'temporal' estimates, set time window to size 1 ");
    	}
    }
    
    if (outPhraseTableBPL != null) {
      outPhraseTableBPL = outDir + "/" + outPhraseTableBPL; 
    }
    if (outPhraseTablePL != null) {
      outPhraseTablePL = outDir + "/" + outPhraseTablePL; 
    }
    if (outPhraseTableP != null) {
      outPhraseTableP = outDir + "/" + outPhraseTableP; 
    }    
    if (outPhraseTableL != null) {
      outPhraseTableL = outDir + "/" + outPhraseTableL; 
    }
    if (outPhraseTableNone != null) {
      outPhraseTableNone = outDir + "/" + outPhraseTableNone; 
    }
    
    LOG.info("--- Preparing for estimating monolingual features " + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForChunkFeaturesCollection();
    PhraseTable phraseTable = preparer.getPhraseTable();
    int chunkNum = 0;
    Set<Phrase> chunk, srcChunkToProcess, trgChunkToProcess;
    
    //if ("wikitemp".equals(Configurator.CONFIG.getString("preprocessing.input.Time"))){
    //	boolean wikiok = preparer.checkWikiTemp();
    //	if (wikiok==false){
    //		throw new Exception("Wikipedia files must match for 'temporal' scoring");
    //	}
    //}

    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    
    SimpleDictionary translitDict = preparer.getTranslitDict();
    Set<Phrase> singleTokenSrcPhrases = phraseTable.getAllSingleTokenSrcPhrases();
    Set<Phrase> singleTokenTrgPhrases = phraseTable.getAllSingleTokenTrgPhrases();
    FeatureEstimator featEstimator = approxFeats ?
        new FeatureEstimator(phraseTable, singleTokenSrcPhrases, singleTokenTrgPhrases, numMonoScoringThreads, new LSHScorer(LSHContext.class),  new LSHScorer(LSHTimeDistribution.class), translitDict, collectPhraseFeats, collectLexFeats) :
        new FeatureEstimator(phraseTable, singleTokenSrcPhrases, singleTokenTrgPhrases, numMonoScoringThreads, contextScorer, timeScorer, translitDict, collectPhraseFeats, collectLexFeats);
    
    // Prepare for single tokens first (we are going to need them when estimating for longer phrases)
    preparer.collectPropsForFeaturesOnly(singleTokenSrcPhrases, singleTokenTrgPhrases);
    // Pre-process properties (i.e. project contexts, normalizes distributions)
    preparer.prepareContextAndTimeProps(true, singleTokenSrcPhrases, contextScorer, timeScorer, approxFeats);
    preparer.prepareContextAndTimeProps(false, singleTokenTrgPhrases, contextScorer, timeScorer, approxFeats);
    
    // Split up the phrase table and process one chunk at a time
    while ((chunk = preparer.getNextChunk(chunkSize)) != null) {
      
      LOG.info(" - Preparing chunk " + (chunkNum++) + " of phrase table ...");
      
      (srcChunkToProcess = new HashSet<Phrase>(chunk)).removeAll(singleTokenSrcPhrases);
      (trgChunkToProcess = new HashSet<Phrase>(phraseTable.getTrgPhrases(chunk))).removeAll(singleTokenTrgPhrases);
    
      preparer.collectPropsForFeaturesOnly(srcChunkToProcess, trgChunkToProcess);
      // Pre-process properties (i.e. project contexts, normalizes distributions)
      preparer.prepareContextAndTimeProps(true, srcChunkToProcess, contextScorer, timeScorer, approxFeats);
      preparer.prepareContextAndTimeProps(false, trgChunkToProcess, contextScorer, timeScorer, approxFeats);
      
      LOG.info(" - Estimating monolingual features for phrase table chunk " + (chunkNum-1) + "...");
      
      // Estimate monolingual similarity features
      featEstimator.estimateFeatures(chunk);
    
      // Save the new phrase tables
      phraseTable.savePhraseTableChunk(chunk, outPhraseTableBPL, FEATS_BPL);
      phraseTable.savePhraseTableChunk(chunk, outPhraseTablePL, FEATS_PL);
      phraseTable.savePhraseTableChunk(chunk, outPhraseTableP, FEATS_P);
      phraseTable.savePhraseTableChunk(chunk, outPhraseTableL, FEATS_L);
      phraseTable.savePhraseTableChunk(chunk, outPhraseTableNone, FEATS_NONE);
      
      // Save stats for Ben
      //phraseTable.saveContextStatsForBen(outDir + "/statsForBen.txt");
      
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
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.features.MonoScoringThreads");
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;
    String outPhraseTablePL = Configurator.CONFIG.getString("output.PhraseTablePL");
    String outPhraseTableP = Configurator.CONFIG.getString("output.PhraseTableP");
    String outPhraseTableL = Configurator.CONFIG.getString("output.PhraseTableL");
    boolean collectPhraseFeats = (outPhraseTablePL != null) || (outPhraseTableP != null);
    boolean collectLexFeats = (outPhraseTablePL != null) || (outPhraseTableL != null);
    boolean approxFeats = Configurator.CONFIG.getBoolean("preprocessing.phrases.features.ApproxFeatsWithLSH");
 
    ///If Wikipedia-Temporal features, time window MUST be 1
    if ("wikitemp".equals(Configurator.CONFIG.getString("preprocessing.input.Time"))){
    	if (windowSize!=1){
    		windowSize=1;
    		LOG.info("WARNING: Using Wikipedia 'temporal' estimates, set time window to size 1 ");
    	}
    }
    
    if (outPhraseTablePL != null) {
      outPhraseTablePL = outDir + "/" + outPhraseTablePL; 
    }
    if (outPhraseTableP != null) {
      outPhraseTableP = outDir + "/" + outPhraseTableP; 
    }    
    if (outPhraseTableL != null) {
      outPhraseTableL = outDir + "/" + outPhraseTableL; 
    }
    
    LOG.info("--- Preparing for estimating monolingual features " + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForChunkFeaturesCollectionForAnni(chunkSize);
    int chunkNum = 0;
    Set<Phrase> srcChunkToProcess, trgChunkToProcess;
    

    //if ("wikitemp".equals(Configurator.CONFIG.getString("preprocessing.input.Time"))){
    //	boolean wikiok = preparer.checkWikiTemp();
    //	if (wikiok==false){
    //		throw new Exception("Wikipedia files must match for 'temporal' scoring");
    //	}
    //}       
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    SimpleDictionary translitDict = preparer.getTranslitDict();
    
    // Split up the phrase table and process one chunk at a time
    while (preparer.readNextChunkForAnni(chunkSize, chunkNum++) > 0) {
            
      srcChunkToProcess = preparer.getPhraseTable().getAllSrcPhrases();
      trgChunkToProcess = preparer.getPhraseTable().getAllTrgPhrases();
    
      preparer.collectPropsForFeaturesOnly(srcChunkToProcess, trgChunkToProcess);
      // Pre-process properties (i.e. project contexts, normalizes distributions)
      preparer.prepareContextAndTimeProps(true, srcChunkToProcess, contextScorer, timeScorer, approxFeats);
      preparer.prepareContextAndTimeProps(false, trgChunkToProcess, contextScorer, timeScorer, approxFeats);
      
      LOG.info(" - Estimating monolingual features for phrase table chunk " + (chunkNum-1) + "...");
      
      if (approxFeats) {
        (new FeatureEstimator(preparer.getPhraseTable(), numMonoScoringThreads, new LSHScorer(LSHContext.class),  new LSHScorer(LSHTimeDistribution.class), translitDict, collectPhraseFeats, collectLexFeats)).estimateFeatures(srcChunkToProcess);
      } else {
        (new FeatureEstimator(preparer.getPhraseTable(), numMonoScoringThreads, contextScorer, timeScorer, translitDict, collectPhraseFeats, collectLexFeats)).estimateFeatures(srcChunkToProcess);
      }
      
      // Save the new phrase tables
      preparer.getPhraseTable().savePhraseTableChunk(srcChunkToProcess, outPhraseTablePL, FEATS_PL);
      preparer.getPhraseTable().savePhraseTableChunk(srcChunkToProcess, outPhraseTableP, FEATS_P);
      preparer.getPhraseTable().savePhraseTableChunk(srcChunkToProcess, outPhraseTableL, FEATS_L);      
    }    
  }
  
  protected void scorePhraseFeaturesAndOrder() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    String outReorderingTableM = Configurator.CONFIG.getString("output.ReorderingTableM");
    int numReorderingThreads = Configurator.CONFIG.getInt("preprocessing.phrases.reordering.ReorderingThreads");
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.features.MonoScoringThreads");
    String outPhraseTableBPL = Configurator.CONFIG.getString("output.PhraseTableBPL");
    String outPhraseTablePL = Configurator.CONFIG.getString("output.PhraseTablePL");
    String outPhraseTableP = Configurator.CONFIG.getString("output.PhraseTableP");
    String outPhraseTableL = Configurator.CONFIG.getString("output.PhraseTableL");
    String outPhraseTableNone = Configurator.CONFIG.getString("output.PhraseTableNone");
    boolean collectPhraseFeats = (outPhraseTableBPL != null) || (outPhraseTablePL != null) || (outPhraseTableP != null);
    boolean collectLexFeats = (outPhraseTableBPL != null) || (outPhraseTablePL != null) || (outPhraseTableL != null);
    boolean approxFeats = Configurator.CONFIG.getBoolean("preprocessing.phrases.features.ApproxFeatsWithLSH");
    boolean approxOrder = Configurator.CONFIG.getBoolean("preprocessing.phrases.reordering.ApproxReordWithLSH");
          
    ///If Wikipedia-Temporal features, time window MUST be 1
    if ("wikitemp".equals(Configurator.CONFIG.getString("preprocessing.input.Time"))){
    	if (windowSize!=1){
    		windowSize=1;
    		LOG.info("WARNING: Using Wikipedia 'temporal' estimates, set time window to size 1 ");
    	}
    }
    
    if (outPhraseTableBPL != null) {
      outPhraseTableBPL = outDir + "/" + outPhraseTableBPL; 
    }
    if (outPhraseTablePL != null) {
      outPhraseTablePL = outDir + "/" + outPhraseTablePL; 
    }
    if (outPhraseTableP != null) {
      outPhraseTableP = outDir + "/" + outPhraseTableP; 
    }    
    if (outPhraseTableL != null) {
      outPhraseTableL = outDir + "/" + outPhraseTableL; 
    }
    if (outPhraseTableNone != null) {
      outPhraseTableNone = outDir + "/" + outPhraseTableNone; 
    }

    PhrasePreparer preparer = new PhrasePreparer();    
    preparer.prepareForFeaturesAndOrderCollection();

    //if ("wikitemp".equals(Configurator.CONFIG.getString("preprocessing.input.Time"))){
    //	boolean wikiok = preparer.checkWikiTemp();
    //	if (wikiok==false){
    //		throw new Exception("Wikipedia files must match for 'temporal' scoring");
  	// }
    //}    
    
    PhraseTable phraseTable = preparer.getPhraseTable();    
    Set<Phrase> srcPhrases = phraseTable.getAllSrcPhrases();
    Set<Phrase> trgPhrases = phraseTable.getAllTrgPhrases();
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    
    SimpleDictionary translitDict = preparer.getTranslitDict();
    
    LOG.info("--- Estimating monolingual features ---");

    // Pre-process properties (i.e. project contexts, normalizes distributions)
    preparer.prepareContextAndTimeProps(true, srcPhrases, contextScorer, timeScorer, approxFeats);
    preparer.prepareContextAndTimeProps(false, trgPhrases, contextScorer, timeScorer, approxFeats);
    
    // Estimate monolingual similarity features
    if (approxFeats) {
      (new FeatureEstimator(phraseTable, numMonoScoringThreads,  new LSHScorer(LSHContext.class),  new LSHScorer(LSHTimeDistribution.class), translitDict, collectPhraseFeats, collectLexFeats)).estimateFeatures(srcPhrases);      
    } else {
      (new FeatureEstimator(phraseTable, numMonoScoringThreads, contextScorer, timeScorer, translitDict, collectPhraseFeats, collectLexFeats)).estimateFeatures(srcPhrases);
    }
    
    // Save the new phrase tables
    phraseTable.savePhraseTable(outPhraseTableBPL, FEATS_BPL);
    phraseTable.savePhraseTable(outPhraseTablePL, FEATS_PL);
    phraseTable.savePhraseTable(outPhraseTableP, FEATS_P);
    phraseTable.savePhraseTable(outPhraseTableL, FEATS_L);
    phraseTable.savePhraseTable(outPhraseTableNone, FEATS_NONE);
    
    // Save stats for Ben
    //phraseTable.saveContextStatsForBen(outDir + "/statsForBen.txt");
    
    LOG.info("--- Estimating reordering features ---");
    preparer.pruneMostFrequentContext(true, srcPhrases);
    preparer.pruneMostFrequentContext(false, trgPhrases);
    
    // Pre-process ordering properties (for LSH)
    preparer.prepareOrderProps(true, srcPhrases, approxOrder);
    preparer.prepareOrderProps(false, trgPhrases, approxOrder);
    
    // Estimate reordering features
    if (approxOrder) {
      (new OrderEstimator(numReorderingThreads, new LSHMonoScorer(), phraseTable, preparer.getMaxTrgPhrCount())).estimateReordering(srcPhrases);
    } else {
      (new OrderEstimator(numReorderingThreads, new MonoScorer(phraseTable), phraseTable, preparer.getMaxTrgPhrCount())).estimateReordering(srcPhrases);
    }
    
    // Save the reordering table
    phraseTable.saveReorderingTable(outDir + "/" + outReorderingTableM);    
  }
}
