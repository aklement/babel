package main.phrases;

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
import babel.util.config.Configurator;
import babel.util.dict.SimpleDictionary;

public class AllPairsTokenScorer {
 
  protected static final Log LOG = LogFactory.getLog(AllPairsTokenScorer.class);
  protected static PairFeat[] FEATS_PL = new PairFeat[]{PairFeat.PHPENALTY, PairFeat.PH_CONTEXT, PairFeat.PH_TIME, PairFeat.LEX_CONTEXT, PairFeat.LEX_TIME, PairFeat.LEX_EDIT};

  public static void main(String[] args) throws Exception {
    
    LOG.info("\n" + Configurator.getConfigDescriptor());
 
    LOG.info("===> Estimating  monolingual features only <===");
    (new AllPairsTokenScorer()).scorePhraseFeaturesOnly();
    LOG.info("===> Done <===");
  }

  protected void scorePhraseFeaturesOnly() throws Exception
  {
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");
    String outDir = Configurator.CONFIG.getString("output.Path");
    int numMonoScoringThreads = Configurator.CONFIG.getInt("preprocessing.phrases.features.MonoScoringThreads");
    String outPhraseTableP = Configurator.CONFIG.getString("output.PhraseTableP");
    boolean approxFeats = Configurator.CONFIG.getBoolean("preprocessing.phrases.features.ApproxFeatsWithLSH");
    int chunkSrcSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;

    assert (outPhraseTableP != null);
    outPhraseTableP = outDir + "/" + outPhraseTableP;   

    LOG.info("--- Preparing for estimating monolingual features ---");
    
    AllPairsTokenPreparer preparer = new AllPairsTokenPreparer();    
    preparer.prepareForFeaturesCollection();
    
    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);    
    SimpleDictionary translitDict = preparer.getTranslitDict();

    Set<Phrase> srcPhrases = preparer.getSrcPhrases();
    Set<Phrase> trgPhrases = preparer.getTrgPhrases();

    // Collect and prepare properties (i.e. project contexts, normalizes distributions)
    preparer.collectContextAndTimeProps(srcPhrases, trgPhrases);
    preparer.prepareContextAndTimeProps(true, srcPhrases, contextScorer, timeScorer, approxFeats);
    preparer.prepareContextAndTimeProps(false, trgPhrases, contextScorer, timeScorer, approxFeats);
    
    PhraseTable chunk;
    AllPairsTokenFeatureEstimator featEstimator;
    int chunkNum = 0;
    
    // Split up the phrase table and process one chunk at a time
    while ((chunk = preparer.getNextChunkToProcess(chunkSrcSize)) != null) {

      LOG.info("--- Estimating monolingual features for chunk " + (chunkNum++) + " with " + chunk.numPhrasePairs() + " phrase pairs ---");
  
      featEstimator = approxFeats ?
          new AllPairsTokenFeatureEstimator(chunk, numMonoScoringThreads, new LSHScorer(LSHContext.class),  new LSHScorer(LSHTimeDistribution.class), translitDict) :
          new AllPairsTokenFeatureEstimator(chunk, numMonoScoringThreads, contextScorer, timeScorer, translitDict);

      // Estimate monolingual similarity features
      featEstimator.estimateFeatures(chunk.getAllSrcPhrases());
    
      chunk.savePhraseTableChunk(chunk.getAllSrcPhrases(), outPhraseTableP, FEATS_PL);
    }    
  }
}
  