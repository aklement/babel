package main;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.TimeDistribution;
import babel.content.eqclasses.properties.Context.ScoreComparator;

import babel.ranking.EquivClassCandRanking;
import babel.ranking.EquivClassPairsRanking;
import babel.ranking.MRRAggregator;
import babel.ranking.Ranker;
import babel.ranking.Reranker;

import babel.ranking.scorers.Scorer;
import babel.ranking.scorers.timedistribution.TimeDistributionCosineScorer;
import babel.ranking.scorers.context.DictScorer;
//import babel.ranking.scorers.context.RappScorer;
//import babel.ranking.scorers.context.BinaryScorer;
//import babel.ranking.scorers.context.CountRatioScorer;
//import babel.ranking.scorers.context.FungS0Scorer;
import babel.ranking.scorers.context.FungS1Scorer;
import babel.ranking.scorers.edit.EditDistanceScorer;
//import babel.ranking.scorers.context.FungS2Scorer;
//import babel.ranking.scorers.context.FungS3Scorer;
//import babel.ranking.scorers.context.TFIDFScorer;

import babel.util.config.Configurator;
import babel.util.dict.Dictionary;

public class NBestCollector 
{
  public static final Log LOG = LogFactory.getLog(NBestCollector.class);
 
  public static void main(String[] args) throws Exception
  {
    LOG.info("\n" + Configurator.getConfigDescriptor());
    
    //int numIter = Configurator.CONFIG.getInt("experiments.Iterations");
    //String nbestFileName = Configurator.CONFIG.containsKey("output.NBestTurk") ? Configurator.CONFIG.getString("output.NBestTurk") : null;
    int maxNumTrgPerSrc = Configurator.CONFIG.getInt("experiments.NumTranslationsToAddPerSource");
    String outDir = Configurator.CONFIG.getString("output.Path");
    int numThreads = Configurator.CONFIG.getInt("experiments.NumRankingThreads");

    NBestCollector collector = new NBestCollector();
    DataPreparer preparer = new DataPreparer();
    
    // Prepare data and resources
    preparer.prepare();
      
    // Select a subset of src classes to actually induct (e.g. most frequent, rand test dictionary, etc...)
    //Set<EquivalenceClass> srcSubset = collector.selectMostFrequentSrcTokens(preparer.getTestDict(), preparer.getSrcEqs(), outDir + "srcwords.txt"); <--    
    Set<EquivalenceClass> srcSubset = collector.selectRandTestDictSrcTokens(preparer.getTestDict(), preparer.getSrcEqs(), outDir + "srcwords.txt");    
    int iterDictSize = maxNumTrgPerSrc * srcSubset.size();         
    
    // Setup scorers
    DictScorer contextScorer = new FungS1Scorer(preparer.getMaxSrcTokCount(), preparer.getMaxTrgTokCount());
    contextScorer.setDict(preparer.m_seedDict);
    Scorer timeScorer = new TimeDistributionCosineScorer();
    Scorer editScorer = new EditDistanceScorer();

    // Pre-process properties
    collector.scoreContexts(srcSubset, preparer.getTrgEqs(), contextScorer);
    collector.normalizeDistros(srcSubset, preparer.getTrgEqs());
    
    Collection<EquivClassCandRanking> cands;
    Set<Collection<EquivClassCandRanking>> allCands = new HashSet<Collection<EquivClassCandRanking>>();
    EquivClassPairsRanking pairsRanking;
    
    LOG.info("Ranking candidates using time...");  
    cands = collector.rank(timeScorer, preparer, srcSubset, maxNumTrgPerSrc, numThreads);
    EquivClassCandRanking.dumpToFile(preparer.m_testDict, cands, outDir + "time.scored");
    pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
    EquivClassPairsRanking.dumpToFile(pairsRanking, outDir + "time.pairs");                  
    allCands.add(cands);

    LOG.info("Re-ranking time candidates using context...");
    cands = collector.reRank(contextScorer, cands);
    pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
    EquivClassPairsRanking.dumpToFile(pairsRanking, outDir + "time-context.pairs");
    
    LOG.info("Ranking candidates using context...");
    cands = collector.rank(contextScorer, preparer, srcSubset, maxNumTrgPerSrc, numThreads);
    EquivClassCandRanking.dumpToFile(preparer.m_testDict, cands, outDir + "context.scored");
    pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
    EquivClassPairsRanking.dumpToFile(pairsRanking, outDir + "context.pairs");    
    allCands.add(cands);
      
    LOG.info("Re-ranking context candidates using time...");
    cands = collector.reRank(timeScorer, cands);
    pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
    EquivClassPairsRanking.dumpToFile(pairsRanking, outDir + "context-time.pairs");
      
    LOG.info("Ranking candidates using edit distance...");  
    cands = collector.rank(editScorer, preparer, srcSubset, maxNumTrgPerSrc, numThreads);
    EquivClassCandRanking.dumpToFile(preparer.m_testDict, cands, outDir + "edit.scored");
    pairsRanking = new EquivClassPairsRanking(true, iterDictSize, cands, maxNumTrgPerSrc);
    EquivClassPairsRanking.dumpToFile(pairsRanking, outDir + "edit.pairs"); 
    allCands.add(cands);
      
    LOG.info("Aggregating (MRR) all rankings...");  
    MRRAggregator aggregator = new MRRAggregator();
    cands =  aggregator.aggregate(allCands);
    EquivClassCandRanking.dumpToFile(preparer.m_testDict, cands, outDir + "aggmrr.scored");
    pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
    EquivClassPairsRanking.dumpToFile(pairsRanking, outDir + "aggmrr.pairs"); 

    LOG.info("--- Done ---");
  }
  
  protected Collection<EquivClassCandRanking> rank(Scorer scorer, DataPreparer preparer, Set<EquivalenceClass> srcSubset, int maxNumberPerSrc, int numThreads) throws Exception
  {     
    Ranker ranker = new Ranker(scorer, maxNumberPerSrc, numThreads);    
    return ranker.getBestCandLists(srcSubset, preparer.getTrgEqs());
  }
 
  protected Collection<EquivClassCandRanking> reRank(Scorer scorer, Collection<EquivClassCandRanking> cands)
  {
    Reranker reranker = new Reranker(scorer);    
    return reranker.reRank(cands);
  }  
  
  /**
   * Selects most frequent source tokens for induction.
   */
  protected Set<EquivalenceClass> selectMostFrequentSrcTokens(Dictionary testDict, Set<EquivalenceClass> srcEqs, String fileName) throws IOException
  {
    int numToKeep = Configurator.CONFIG.containsKey("experiments.NumSource") ? Configurator.CONFIG.getInt("experiments.NumSource") : -1;
    Set<EquivalenceClass> srcSubset = new HashSet<EquivalenceClass>(srcEqs);
    
    if ((numToKeep >= 0) && (srcSubset.size() > numToKeep))
    {
      LinkedList<EquivalenceClass> valList = new LinkedList<EquivalenceClass>(srcSubset);
      Collections.sort(valList, new CountComparator());
      
      for (int i = numToKeep; i < valList.size(); i++)
      {
        srcSubset.remove(valList.get(i));
      }
    }
    
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for (EquivalenceClass eq : srcSubset)
    { writer.write(eq.toString() + "\t" + ((Number)eq.getProperty(Number.class.getName())).getNumber() + "\n");
    }
    
    writer.close();
    
    LOG.info("Selected " + srcSubset.size() + " most frequent source classes (see " + fileName + ").");

    return srcSubset;
  }

  /**
   * Selects random test dictionary tokens for induction.
   */
  protected Set<EquivalenceClass> selectRandTestDictSrcTokens(Dictionary testDict, Set<EquivalenceClass> srcEqs, String fileName) throws Exception
  {
    int numToKeep = Configurator.CONFIG.containsKey("experiments.NumSource") ? Configurator.CONFIG.getInt("experiments.NumSource") : -1;
    Set<EquivalenceClass> srcSubset = new HashSet<EquivalenceClass>(srcEqs);
    
    srcSubset.retainAll(testDict.getAllKeys());
    
    if ((numToKeep >= 0) && (srcSubset.size() > numToKeep))
    {
      LinkedList<EquivalenceClass> valList = new LinkedList<EquivalenceClass>(srcSubset);
      srcSubset.clear();

      for (int i = 0; i < numToKeep; i++)
      { srcSubset.add(valList.remove(m_rand.nextInt(valList.size())));
      }
    }
        
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for (EquivalenceClass eq : srcSubset)
    { writer.write(eq.toString() + "\t" + ((Number)eq.getProperty(Number.class.getName())).getNumber() + "\n");
    }
    
    writer.close();
    
    LOG.info("Selected " + srcSubset.size() + " random test dictionary source classes (see " + fileName + ").");

    return srcSubset;
  }
  
  protected void pruneContextsAccordingToScore(Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs, DictScorer scorer)
  {
    ScoreComparator comparator = new ScoreComparator(scorer);
    int pruneContextEqs = Configurator.CONFIG.getInt("experiments.context.PruneContextToSize");

    // Prune context
    for (EquivalenceClass ec : srcEqs)
    { ((Context)ec.getProperty(Context.class.getName())).pruneContext(pruneContextEqs, comparator);
    }
    
    for (EquivalenceClass ec : trgEqs)
    { ((Context)ec.getProperty(Context.class.getName())).pruneContext(pruneContextEqs, comparator);
    }
  }
  
  protected void scoreContexts(Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs, DictScorer scorer)
  {
    LOG.info("Scoring contextual items with " + scorer.toString() + "...");

    // Score contextual items
    for (EquivalenceClass ec : srcEqs)
    { ((Context)ec.getProperty(Context.class.getName())).scoreContextualItems(scorer);
    }
    
    for (EquivalenceClass ec : trgEqs)
    { ((Context)ec.getProperty(Context.class.getName())).scoreContextualItems(scorer);
    }
  }
  
  protected void normalizeDistros(Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs)
  {
    LOG.info("Normalizing time distributions...");

    // Score contextual items
    for (EquivalenceClass ec : srcEqs)
    { ((TimeDistribution)ec.getProperty(TimeDistribution.class.getName())).normalize();
    }
    
    for (EquivalenceClass ec : trgEqs)
    { ((TimeDistribution)ec.getProperty(TimeDistribution.class.getName())).normalize();
    }
  }  
  
  protected void outputMTurkData(String fileName, Dictionary iterDict, Dictionary goldDict, int numTrgPerSrc) throws Exception
  {
    OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(fileName));
    List<EquivalenceClass> trgCands;
    List<EquivalenceClass> goldSrcEqs = new LinkedList<EquivalenceClass>(goldDict.getAllKeys());
    int numCands, numProduced;
    EquivalenceClass otherSrcEq, trgCand;
    boolean hasPos, inDict;
    Random rand = new Random(1);
    
    // 1. Outputs entire iterDict
    // 2. Each translation is flagged whether it is correct (1, in the goldDict), unknown (0, not in the goldDict), or incorrect (-1)
    // 3. There are zero or more correct trenalations (marked 1)
    // 4. There are one or more incorrect translations (marked -1)
    // 5. There are always exactly numTrgPerSrc translations
    
    for (EquivalenceClass srcEq : iterDict.getAllKeys())
    {
      trgCands = iterDict.getTranslations(srcEq);
      numCands = numTrgPerSrc;
      
      // Get a negative example first
      while (srcEq.overlap(otherSrcEq = goldSrcEqs.get(rand.nextInt(goldSrcEqs.size()))) || (!goldDict.hasTranslations(otherSrcEq)));
      out.write(srcEq.toString() + " " + goldDict.getRandomTranslation(otherSrcEq).toString() + " -1 ");
      numCands--;
      
      // Get a positive example if       
      if (inDict = goldDict.containsKey(srcEq))
      {
        hasPos = false;

        for (EquivalenceClass trg : trgCands)
        {
          if (goldDict.checkTranslation(srcEq,  trg))
          {
            hasPos = true;
            out.write(trg.toString() + " +1 ");
            trgCands.remove(trg);
            break;
          }
        }
        
        if (!hasPos)
        { out.write(goldDict.getRandomTranslation(srcEq).toString() + " +2 ");
        }
        
        numCands--;
      }
      
      // Get the translation canidates
      numProduced = Math.min(trgCands.size(), numCands);

      for (int c = 0; c < numProduced; c++)
      {
        trgCand = trgCands.get(c);
        out.write(trgCand.toString() + (inDict && goldDict.checkTranslation(srcEq, trgCand) ? " +1 " : " 0 "));
      }
      
      // Fill the rest with negatives
      for (int c = 0; c < numCands - numProduced; c++)
      {
        while (srcEq.overlap(otherSrcEq = goldSrcEqs.get(rand.nextInt(goldSrcEqs.size()))) || (!goldDict.hasTranslations(otherSrcEq)));
        out.write(goldDict.getRandomTranslation(otherSrcEq).toString() + " -1 ");
      }
      
      out.write("\n");
    }
    
    out.close();
  }

  protected Random m_rand = new Random(1);
  
  public static class CountComparator implements Comparator<EquivalenceClass>
  {
    public int compare(EquivalenceClass item1, EquivalenceClass item2)
    {
      double diff = ((Number)item2.getProperty(Number.class.getName())).getNumber() - ((Number)item1.getProperty(Number.class.getName())).getNumber();
      
      return (diff == 0) ? 0 : (diff < 0) ? -1 : 1;
    }
  }
}