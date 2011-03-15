package main.phrases;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.phrases.Phrase;
import babel.ranking.scorers.Scorer;
import babel.ranking.scorers.context.DictScorer;
import babel.ranking.scorers.context.FungS1Scorer;
import babel.ranking.scorers.timedistribution.TimeDistributionCosineScorer;
import babel.util.config.Configurator;

public class BenPhraseScorer {
  protected static final Log LOG = LogFactory.getLog(BenPhraseScorer.class);

  protected static final String SRC_TO_INDUCT = "src.list";
  protected static final String TRG_TO_INDUCT = "trg.list";

  protected static final String SRC_PHRASES = "src.phrases";
  protected static final String TRG_PHRASES = "trg.phrases";
  protected static final String SRC_CONTEXT = "src.context.bin";
  protected static final String TRG_CONTEXT = "trg.context.bin";
  protected static final String SRC_TIME = "src.time.bin";
  protected static final String TRG_TIME = "trg.time.bin";
  
  public static void main(String[] args) throws Exception {
    
    LOG.info("\n" + Configurator.getConfigDescriptor());    
    BenPhraseScorer scorer = new BenPhraseScorer();
   
    LOG.info("===> Scoring phrases <===");
    scorer.scorePhrases();
    LOG.info("===> Done <===");
  }

  protected void scorePhrases() throws Exception
  {
    String outDir = Configurator.CONFIG.getString("output.Path");
    String inDir = Configurator.CONFIG.getString("preprocessing.Path");
    
    String srcPhraseFile = inDir + "/" + SRC_TO_INDUCT;
    String trgPhraseFile = inDir + "/" + TRG_TO_INDUCT;

    String srcPhraseOutFile = outDir + "/" + SRC_PHRASES;
    String trgPhraseOutFile = outDir + "/" + TRG_PHRASES;
    String srcContextFile = outDir + "/" + SRC_CONTEXT;
    String trgContextFile = outDir + "/" + TRG_CONTEXT;
    String srcTimeFile = outDir + "/" + SRC_TIME;
    String trgTimeFile = outDir + "/" + TRG_TIME;    
        
    LOG.info("--- Preparing for collecting signatures ---");
    
    BenPhrasePreparer preparer = new BenPhrasePreparer();
    preparer.prepareContextForChunkCollection();

    scoreChunks(preparer, true, srcPhraseFile, srcPhraseOutFile, srcContextFile, srcTimeFile);
    scoreChunks(preparer, false, trgPhraseFile, trgPhraseOutFile, trgContextFile, trgTimeFile);
  }
  
  protected void scoreChunks(BenPhrasePreparer preparer, boolean src, String inFileName, String outFileNamePhrases, String outFileNameContext, String outFileNameTime) throws Exception {
    
    int chunkSize = (Configurator.CONFIG.containsKey("preprocessing.phrases.ChunkSize") && Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") > 0) ? Configurator.CONFIG.getInt("preprocessing.phrases.ChunkSize") : Integer.MAX_VALUE;    
    boolean slidingWindow = Configurator.CONFIG.getBoolean("experiments.time.SlidingWindow");
    int windowSize = Configurator.CONFIG.getInt("experiments.time.WindowSize");

    LOG.info("--- Collecting signatures for" + (src ? " source " : " target ") + (chunkSize != Integer.MAX_VALUE ? "in chunks of size " + chunkSize : "") + " ---");
    
    Set<Phrase> chunk;
    int chunkNum = 0;

    DictScorer contextScorer = new FungS1Scorer(preparer.getSeedDict(), preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    Scorer timeScorer = new TimeDistributionCosineScorer(windowSize, slidingWindow);
    
    preparer.prepareForChunkCollection(src, inFileName, chunkSize); 
    preparer.openFiles(outFileNamePhrases, outFileNameContext, outFileNameTime);
    
    while ((chunk = preparer.prepareNextChunk(src, inFileName, chunkSize)).size() > 0) {

      LOG.info(" - Generating signatures for chunk " + (chunkNum++) + "...");

      // Collect context and time properties for chunk
      preparer.collectContextAndTimeProps(src, chunk);
      // Pre-process properties (i.e. project contexts, normalizes distributions, and generate signatures)
      preparer.prepareContextAndTimeProps(src, chunk, contextScorer, timeScorer);
      // Save the phrase table chunk
      preparer.saveChunk(chunk);
    }
    
    preparer.closeFiles();
  }
}
