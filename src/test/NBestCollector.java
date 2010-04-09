package test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.corpora.accessors.EuroParlCorpusAccessor;
import babel.content.corpora.accessors.LexCorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.content.eqclasses.collectors.EquivalenceClassCollector;
import babel.content.eqclasses.collectors.SimpleEquivalenceClassCollector;
import babel.content.eqclasses.filters.DictionaryFilter;
import babel.content.eqclasses.filters.EquivalenceClassFilter;
import babel.content.eqclasses.filters.GarbageFilter;
import babel.content.eqclasses.filters.NoContextFilter;
import babel.content.eqclasses.filters.NoTimeDistributionFilter;
import babel.content.eqclasses.filters.NumOccurencesFilter;
import babel.content.eqclasses.filters.RomanizationFilter;
import babel.content.eqclasses.filters.LengthFilter;
import babel.content.eqclasses.filters.StopWordsFilter;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.ContextCollector;
import babel.content.eqclasses.properties.NumberAndTypeCollector;
import babel.content.eqclasses.properties.TimeDistribution;
import babel.content.eqclasses.properties.TimeDistributionCollector;
import babel.content.eqclasses.properties.Context.ScoreComparator;
import babel.content.eqclasses.properties.Type.EqType;

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
import babel.util.dict.Dictionary.DictPair;
import babel.util.persistence.EqClassPersister;

// TODO: Save Eq Classes + Properties - Build a map from Eq Class to an ID
public class NBestCollector 
{
  public static final Log LOG = LogFactory.getLog(NBestCollector.class);
 
  public static void main(String[] args) throws Exception
  {
    LOG.info("\n" + Configurator.getConfigDescriptor());
    
    // Number of iterations
    int numIter = Configurator.CONFIG.getInt("experiments.Iterations");
    // Number of translations per Source word to add to dictionary (up to k)
    int maxNumTrgPerSrc = Configurator.CONFIG.getInt("experiments.NumTranslationsToAddPerSource");
    // Output directory
    String outDir = Configurator.CONFIG.getString("output.Path");
    // N-best file in CSV format for Mech Turk
    //String nbestFileName = Configurator.CONFIG.containsKey("output.NBestTurk") ? Configurator.CONFIG.getString("output.NBestTurk") : null;
    // Number of threads
    int numThreads = Configurator.CONFIG.getInt("experiments.NumRankingThreads");

    NBestCollector collector = new NBestCollector();
    Collection<EquivClassCandRanking> cands;
    Set<Collection<EquivClassCandRanking>> allCands = new HashSet<Collection<EquivClassCandRanking>>();
    
    LOG.info("--- Preparing dictionaries ---");
    //collector.prepareDictionaries();  <--
    collector.prepareDictionariesFromSingleFile();
    Dictionary cumulativeResultDict = new Dictionary(SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "CumulativeIteration");  
    Dictionary contextDict;
    //Dictionary iterResultDict;
    int iterDictSize;

    //String iterDictFile, cumulativeDictFile, nbestIterFile;
    EquivClassPairsRanking pairsRanking;
    
    for (int iter = 0; iter < numIter; iter++)
    {
      LOG.info("--- Iteration " + iter + " ---");
      
      // Put together a dictionary for this iteration
      contextDict = collector.m_seedDict.clone();
      contextDict.setName(contextDict.getName() + "_and_iteration_" + iter);    
      contextDict.augment(cumulativeResultDict, false);
      LOG.info("Using for scoring context: " + contextDict.toString());

      // TODO: Expensive to do on every iteration: we re-construct data so that we don't keep non 
      // projectable words around while inducing (but that set changes on each iteration)
      LOG.info("Preparing data...");
      collector.collectAndPrepareEqClasses(contextDict);      
      
      // Select a subset of src classes to actually induct (e.g. most frequent, out of test dictionary, etc...)
      //collector.selectMostFrequentSrcTokens((iter != 0) ? null : outDir + "srcwords.txt"); <--
      collector.selectRandTestDictSrcTokens((iter != 0) ? null : outDir + "srcwords.txt");

      iterDictSize = maxNumTrgPerSrc * collector.m_srcEq.size();      
      
      LOG.info("Ranking candidates using context...");
      
      cands = collector.rankWithContext(contextDict, maxNumTrgPerSrc, numThreads);
      pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
      pairsRanking.dumpToFile(outDir + iter + ".context.pairs");      
      allCands.add(cands);
      
      LOG.info("Re-ranking context candidates using time...");
      
      cands = collector.reRankWithTime(cands);
      pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
      pairsRanking.dumpToFile(outDir + iter + ".context-time.pairs"); 
      
      LOG.info("Ranking candidates using time...");
      
      cands = collector.rankWithTime(maxNumTrgPerSrc, numThreads);
      pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
      pairsRanking.dumpToFile(outDir + iter + ".time.pairs");                  
      allCands.add(cands);
      
      LOG.info("Re-ranking time candidates using context...");
      
      cands = collector.reRankWithContext(contextDict, cands);
      pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
      pairsRanking.dumpToFile(outDir + iter + ".time-context.pairs");
      
      LOG.info("Ranking candidates using edit distance...");
      
      cands = collector.rankWithEdit(maxNumTrgPerSrc, numThreads);
      pairsRanking = new EquivClassPairsRanking(true, iterDictSize, cands, maxNumTrgPerSrc);
      pairsRanking.dumpToFile(outDir + iter + ".edit.pairs"); 
      allCands.add(cands);
      
      LOG.info("Aggregating (MRR) all rankings...");
      
      MRRAggregator aggregator = new MRRAggregator();
      cands =  aggregator.aggregate(allCands);
      pairsRanking = new EquivClassPairsRanking(false, iterDictSize, cands, maxNumTrgPerSrc);
      pairsRanking.dumpToFile(outDir + iter + ".aggmrr.pairs"); 
      
      // Put together an iteration dictionary
      //iterResultDict = new Dictionary(SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Iteration-" + iter);
      //iterResultDict.addAll(pairsRanking);
      //cumulativeResultDict.augment(iterResultDict, true);
      
      //iterDictFile = outDir + iter + ".iter.dict";
      //cumulativeDictFile = outDir + iter + ".iter.cumulative.dict";
      //nbestIterFile = outDir + iter + "." + nbestFileName;
      
      //collector.outputMTurkData(nbestIterFile, iterResultDict, collector.m_useDict, 10);
      //iterResultDict.write(iterDictFile);
      //cumulativeResultDict.write(cumulativeDictFile);
    }

    LOG.info("--- Done ---");
  }
  
  protected Collection<EquivClassCandRanking> rankWithContext(Dictionary contextDict, int maxNumberPerSrc, int numThreads) throws Exception
  {     
    //DictScorer iterScorer = new TFIDFScorer(m_srcContextEq, m_trgContextEq);
    //DictScorer iterScorer = new CountRatioScorer();
    //DictScorer iterScorer = new RappScorer(m_numToksInSrc, m_numToksInTrg);
    DictScorer scorer = new FungS1Scorer(m_maxTokCountInSrc, m_maxTokCountInTrg);
    Ranker ranker = new Ranker(scorer, maxNumberPerSrc, numThreads);
    
    scorer.setDict(contextDict);
    //collector.pruneContextsAccordingToScore(iterScorer); // TODO: Play with this...
    scoreContexts(scorer);
    
    return ranker.getBestCandLists(m_srcEq, m_trgEq);
  }
  
  protected Collection<EquivClassCandRanking> reRankWithContext(Dictionary contextDict, Collection<EquivClassCandRanking> cands)
  {
    DictScorer scorer = new FungS1Scorer(m_maxTokCountInSrc, m_maxTokCountInTrg);
    Reranker reranker = new Reranker(scorer);
    
    scorer.setDict(contextDict);
    scoreContexts(scorer);    
    
    return reranker.reRank(cands);
  }  

  protected Collection<EquivClassCandRanking> rankWithTime(int maxNumberPerSrc, int numThreads) throws Exception
  {
    Scorer scorer = new TimeDistributionCosineScorer();
    Ranker ranker = new Ranker(scorer, maxNumberPerSrc, numThreads);
    
    return ranker.getBestCandLists(m_srcEq, m_trgEq);     
  }
  
  protected Collection<EquivClassCandRanking> reRankWithTime(Collection<EquivClassCandRanking> cands)
  {
    Scorer scorer = new TimeDistributionCosineScorer();
    Reranker reranker = new Reranker(scorer);
    
    return reranker.reRank(cands);
  }
  
  protected Collection<EquivClassCandRanking> rankWithEdit(int maxNumberPerSrc, int numThreads) throws Exception
  {
    Scorer scorer = new EditDistanceScorer();
    Ranker ranker = new Ranker(scorer, maxNumberPerSrc, numThreads);
    
    return ranker.getBestCandLists(m_srcEq, m_trgEq);     
  }
  
  protected Collection<EquivClassCandRanking> reRankWithEdit(Collection<EquivClassCandRanking> cands)
  {
    Scorer scorer = new EditDistanceScorer();
    Reranker reranker = new Reranker(scorer);
    
    return reranker.reRank(cands);
  }  
  
  protected void prepareDictionariesFromSingleFile() throws Exception
  {
    // Dict Directory
    String dictDir = Configurator.CONFIG.getString("resources.dictionaries.Path");

    // Dictionary files
    String useDictFile = Configurator.CONFIG.getString("resources.dictionaries.UseDictionary");
    String augDictFile = Configurator.CONFIG.containsKey("experiments.AugDictionary") ? Configurator.CONFIG.getString("experiments.AugDictionary") : null;

    // Filter out romanized target translations
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("experiments.FilterRomanTrg") && Configurator.CONFIG.getBoolean("experiments.FilterRomanTrg");
    // Prune dictionary items with more than a given number of translations
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
    
    // Load Dictionary
    m_useDict = new Dictionary(dictDir + useDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Usable", filterRomanTrg);
    m_useDict.pruneCounts(ridDictNumTrans);
    LOG.info("Use Dictionary: " + m_useDict.toString());    
    
    // Create seed dictionary
    if (Configurator.CONFIG.containsKey("experiments.DictionaryPercentToUse") && Configurator.CONFIG.containsKey("experiments.DictionaryNumberToUse"))
    { throw new IllegalArgumentException("Both percentage and number defined");
    }
    else if (Configurator.CONFIG.containsKey("experiments.DictionaryPercentToUse"))
    {
      double percentSeedDict = Configurator.CONFIG.getDouble("experiments.DictionaryPercentToUse");
      DictPair dicts = m_useDict.splitPercent(percentSeedDict * 100 + "%-of-Usable", (1.0 - percentSeedDict) * 100 + "%-of-Usable", percentSeedDict);
      m_seedDict = dicts.dict;
      m_testDict = dicts.rest;
    }
    else if (Configurator.CONFIG.containsKey("experiments.DictionaryNumberToUse"))
    {
      int numberSeedDict = Configurator.CONFIG.getInt("experiments.DictionaryNumberToUse");
      DictPair dicts = m_useDict.splitPart(numberSeedDict + "-entries-from-Usable", (m_useDict.size() - numberSeedDict) + "-entries-from-Usable", numberSeedDict);
      m_seedDict = dicts.dict;
      m_testDict = dicts.rest;
    }
    else
    { throw new IllegalArgumentException();
    }
    LOG.info("Seed Dictionary: " + m_seedDict.toString());
    LOG.info("Test Dictionary: " + m_testDict.toString());

    // Augment seed dictionary with aug dict (if given)
    if ((augDictFile != null) && (new File(dictDir + augDictFile).exists()))
    {
      Dictionary augDict = new Dictionary(dictDir + augDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "AugDictionary", filterRomanTrg);
      m_seedDict.augment(augDict, false);
      LOG.info("Seed dictionary augmented (without replacement) with: " + augDict.toString());
      LOG.info("New seed dictionary: " + m_seedDict.toString());
    }
  }

  protected void prepareDictionaries() throws Exception
  {
    // Dict Directory
    String dictDir = Configurator.CONFIG.getString("resources.DictPath");

    // Dictionary files
    String testDictFile = Configurator.CONFIG.getString("resources.dictionaries.TestDictionary");
    String useDictFile = Configurator.CONFIG.getString("resources.dictionaries.UseDictionary");
    String augDictFile = Configurator.CONFIG.containsKey("experiments.AugDictionary") ? Configurator.CONFIG.getString("experiments.AugDictionary") : null;

    // Filter out romanized target translations
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("experiments.FilterRomanTrg") && Configurator.CONFIG.getBoolean("experiments.FilterRomanTrg");
    // Prune dictionary items with more than a given number of translations
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;

    // Load Test Dictionary
    m_testDict = new Dictionary(dictDir + testDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Test", filterRomanTrg);
    m_testDict.pruneCounts(ridDictNumTrans);
    LOG.info("Test Dictionary: " + m_testDict.toString());
    
    // Load Usable Dictionary
    m_useDict = new Dictionary(dictDir + useDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Usable", filterRomanTrg);
    m_useDict.pruneCounts(ridDictNumTrans);
    LOG.info("Use Dictionary: " + m_useDict.toString());    
    
    // Create seed dictionary
    if (Configurator.CONFIG.containsKey("experiments.DictionaryPercentToUse") && Configurator.CONFIG.containsKey("experiments.DictionaryNumberToUse"))
    { throw new IllegalArgumentException("Both percentage and number defined");
    }
    else if (Configurator.CONFIG.containsKey("experiments.DictionaryPercentToUse"))
    {
      double percentSeedDict = Configurator.CONFIG.getDouble("experiments.DictionaryPercentToUse");
      m_seedDict = m_useDict.splitPercent(percentSeedDict * 100 + "%-of-Usable", "", percentSeedDict).dict;
    }
    else if (Configurator.CONFIG.containsKey("experiments.DictionaryNumberToUse"))
    {
      int numberSeedDict = Configurator.CONFIG.getInt("experiments.DictionaryNumberToUse");
      m_seedDict = m_useDict.splitPart(numberSeedDict + "-entries-from-Usable", "", numberSeedDict).dict;
    }
    else
    { throw new IllegalArgumentException();
    }    
    LOG.info("Seed Dictionary: " + m_seedDict.toString());

    // Augment seed dictionary with aug dict (if given)
    if ((augDictFile != null) && (new File(dictDir + augDictFile).exists()))
    {
      Dictionary augDict = new Dictionary(dictDir + augDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "AugDictionary", filterRomanTrg);
      m_seedDict.augment(augDict, false);
      LOG.info("Seed dictionary augmented (without replacement) with: " + augDict.toString());
      LOG.info("New seed dictionary: " + m_seedDict.toString());
    }
  }
  
  protected EuroParlCorpusAccessor getEuroParlAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.europarl.Path"); 
    String subDir = src ? Configurator.CONFIG.getString("corpora.europarl.SrcSubDir") : Configurator.CONFIG.getString("corpora.europarl.TrgSubDir");

    SimpleDateFormat sdf = new SimpleDateFormat( "yy-MM-dd" );
    Date fromDate = sdf.parse(Configurator.CONFIG.getString("corpora.europarl.DateFrom"));
    Date toDate = sdf.parse(Configurator.CONFIG.getString("corpora.europarl.DateTo"));
    
    return new EuroParlCorpusAccessor(path + subDir, fromDate, toDate);
  }
  
  protected LexCorpusAccessor getPlainTextAccessor(boolean src)
  {
    String path = Configurator.CONFIG.getString("corpora.plaintext.Path");
    String fileRegExp = src ? Configurator.CONFIG.getString("corpora.plaintext.SrcRegExp") : Configurator.CONFIG.getString("corpora.plaintext.TrgRegExp");
  
    return new LexCorpusAccessor(fileRegExp, path);
  }
  
  protected void collectAndPrepareEqClasses(Dictionary contextDict) throws Exception
  {
    // Paths
    String stopWordsDir = Configurator.CONFIG.getString("resources.stopwords.Path");
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");  
    
    // Stop word lists
    String srcStop = Configurator.CONFIG.getString("resources.stopwords.SrcStopWords");
    String trgStop = Configurator.CONFIG.getString("resources.stopwords.TrgStopWords");

    // Files to store preprocessing information
    String srcEqClasses = Configurator.CONFIG.getString("preprocessing.SrcEqClasses");
    String trgEqClasses = Configurator.CONFIG.getString("preprocessing.TrgEqClasses");

    int pruneContEqIfOccursFewerThan = Configurator.CONFIG.getInt("experiments.context.PruneEqIfOccursFewerThan");
    int pruneContEqIfOccursMoreThan = Configurator.CONFIG.getInt("experiments.context.PruneEqIfOccursMoreThan");
    int pruneCandIfOccursFewerThan = Configurator.CONFIG.getInt("experiments.candidates.PruneIfOccursFewerThan");
    int pruneCandIfOccursMoreThan = Configurator.CONFIG.getInt("experiments.candidates.PruneIfOccursMoreThan");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("experiments.FilterRomanTrg") && Configurator.CONFIG.getBoolean("experiments.FilterRomanTrg");

    // Get all equivalence classes for source and target (subject to a couple of simple filters)
    LOG.info("Collecting equivalence classes ...");

    ArrayList<EquivalenceClassFilter> filters = new ArrayList<EquivalenceClassFilter>(3);
    filters.add(new GarbageFilter());
    filters.add(new LengthFilter(2));

    CorpusAccessor srcAccessor = getEuroParlAccessor(true);
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(SimpleEquivalenceClass.class.getName(), filters, false);
    m_srcContextEq = collector.collect(srcAccessor.getCorpusReader(), -1);
    
    if (filterRomanTrg)
    { filters.add(new RomanizationFilter());
    }
    CorpusAccessor trgAccessor = getEuroParlAccessor(false);
    collector = new SimpleEquivalenceClassCollector(SimpleEquivalenceClass.class.getName(), filters, false);
    m_trgContextEq = collector.collect(trgAccessor.getCorpusReader(), -1);    

    EqClassPersister.persistEqClasses(m_srcContextEq, preProcDir + srcEqClasses);
    EqClassPersister.persistEqClasses(m_trgContextEq, preProcDir + trgEqClasses); 
    LOG.info("All source classes: " + m_srcContextEq.size());
    LOG.info("All target classes: " + m_trgContextEq.size() + (filterRomanTrg ? " (without romanization) " : ""));

    // Get source and target stop words (if any)
    
    LOG.info("Collecting stop words ...");
    
    collector = new SimpleEquivalenceClassCollector(SimpleEquivalenceClass.class.getName(), null, false);
    Set<EquivalenceClass> srcStopEq = ((new File(stopWordsDir + srcStop)).exists()) ?
      collector.collect((new LexCorpusAccessor(srcStop, stopWordsDir)).getCorpusReader(), -1) :
      new HashSet<EquivalenceClass>();
      
    collector = new SimpleEquivalenceClassCollector(SimpleEquivalenceClass.class.getName(), null, false);
    Set<EquivalenceClass> trgStopEq = ((new File(stopWordsDir + trgStop)).exists()) ? 
      collector.collect((new LexCorpusAccessor(trgStop, stopWordsDir)).getCorpusReader(),-1) :
      new HashSet<EquivalenceClass>();

    LOG.info("Source stop words:" + srcStopEq.size());
    LOG.info("Target stop words:" + trgStopEq.size());
    
    // Collect corpus counts
    
    LOG.info("Collecting corpus counts...");
    
    (new NumberAndTypeCollector(SimpleEquivalenceClass.class.getName(), false, EqType.SOURCE)).collectProperty(srcAccessor, m_srcContextEq);    
    (new NumberAndTypeCollector(SimpleEquivalenceClass.class.getName(), false, EqType.TARGET)).collectProperty(trgAccessor, m_trgContextEq);

    m_srcEq = new HashSet<EquivalenceClass>(m_srcContextEq);
    m_trgEq = new HashSet<EquivalenceClass>(m_trgContextEq);
    
    dictCoverage(contextDict, m_srcEq, true);
    dictCoverage(contextDict, m_trgEq, false);
    collectTokenCounts(m_srcEq, m_trgEq);
    
    // Prune contextual classes
    
    LOG.info("Pruning contextual words: keeping those in dict [" + contextDict.toString() + "] and occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times...");    

    filters.clear();
    filters.add(new DictionaryFilter(contextDict, true, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    m_srcContextEq = EquivalenceClassCollector.filter(m_srcContextEq, filters);

    filters.clear();
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    filters.add(new DictionaryFilter(contextDict, true, false));    
    m_trgContextEq = EquivalenceClassCollector.filter(m_trgContextEq, filters);
    
    LOG.info("Context source classes: " + m_srcContextEq.size());
    LOG.info("Context target classes: " + m_trgContextEq.size());

    // Prune candidate classes
    
    LOG.info("Pruning candidate words: removing stop words and keeping if occur (" + pruneCandIfOccursFewerThan + "," + pruneCandIfOccursMoreThan + ") times..."); 
    
    filters.clear();
    filters.add(new StopWordsFilter(srcStopEq));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursMoreThan, false));
    m_srcEq = EquivalenceClassCollector.filter(m_srcEq, filters);
    
    filters.clear();
    filters.add(new StopWordsFilter(trgStopEq));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursMoreThan, false));
    m_trgEq = EquivalenceClassCollector.filter(m_trgEq, filters);

    // Collect context properties
    
    LOG.info("Collecting context...");

    (new ContextCollector(SimpleEquivalenceClass.class.getName(), false, 2, 2, m_srcContextEq)).collectProperty(srcAccessor, m_srcEq);    
    (new ContextCollector(SimpleEquivalenceClass.class.getName(), false, 2, 2, m_trgContextEq)).collectProperty(trgAccessor, m_trgEq);
    
    LOG.info("Pruning candidate words: removing words with no context..."); 

    filters.clear();
    filters.add(new NoContextFilter());
    m_srcEq = EquivalenceClassCollector.filter(m_srcEq, filters);
    m_trgEq = EquivalenceClassCollector.filter(m_trgEq, filters);
  
    // Collect context properties
    
    LOG.info("Collecting temporal distributions...");
    
    (new TimeDistributionCollector(SimpleEquivalenceClass.class.getName(), false, 1, false)).collectProperty(srcAccessor, m_srcEq);    
    (new TimeDistributionCollector(SimpleEquivalenceClass.class.getName(), false, 1, false)).collectProperty(trgAccessor, m_trgEq);

    LOG.info("Pruning candidate words: removing words with no time distribution..."); 
    
    filters.clear();
    filters.add(new NoTimeDistributionFilter());
    m_srcEq = EquivalenceClassCollector.filter(m_srcEq, filters);
    m_trgEq = EquivalenceClassCollector.filter(m_trgEq, filters);
    
    LOG.info("Candidate source classes: " + m_srcEq.size());
    LOG.info("Candidate target classes: " + m_trgEq.size());
    
    // Save collected propereties
    
    EqClassPersister.persistProperty(m_srcEq, Number.class.getName(), preProcDir + Number.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_trgEq, Number.class.getName(), preProcDir + Number.class.getSimpleName() + ".trg.map");
    EqClassPersister.persistProperty(m_srcEq, Context.class.getName(), preProcDir + Context.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_trgEq, Context.class.getName(), preProcDir + Context.class.getSimpleName() + ".trg.map");
    EqClassPersister.persistProperty(m_srcEq, TimeDistribution.class.getName(), preProcDir + TimeDistribution.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_trgEq, TimeDistribution.class.getName(), preProcDir + TimeDistribution.class.getSimpleName() + ".trg.map");    
  }
  
  /**
   * Selects most frequent source tokens for induction.
   * @throws IOException 
   */
  protected void selectMostFrequentSrcTokens(String fileName) throws IOException
  {
    int numToKeep = Configurator.CONFIG.containsKey("experiments.NumSource") ? Configurator.CONFIG.getInt("experiments.NumSource") : -1;

    LinkedList<EquivalenceClass> valList = new LinkedList<EquivalenceClass>(m_srcEq);
    Collections.sort(valList, new CountComparator());
    
    if ((numToKeep >= 0) && (m_srcEq.size() > numToKeep))
    {
      for (int i = numToKeep; i < valList.size(); i++)
      {
        m_srcEq.remove(valList.get(i));
      }
    }
      
    LOG.info("Looking for translations for " + m_srcEq.size() + " most frequent source classes (see " + fileName + ").");
    
    StringBuilder strBld = new StringBuilder();
    numToKeep = (numToKeep < 0) ? valList.size() : Math.min(numToKeep, valList.size());
    
    for (int i = 0; i < numToKeep; i++)
    { strBld.append(valList.get(i).toString() + "\t" + ((Number)valList.get(i).getProperty(Number.class.getName())).getNumber() + "\n");
    }
    
    LOG.info(strBld.toString());
    
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(strBld.toString());
    writer.close();
  }

  /**
   * Selects random test dictionary tokens for induction.
   */
  protected void selectRandTestDictSrcTokens(String fileName) throws Exception
  {
    int numToKeep = Configurator.CONFIG.containsKey("experiments.NumSource") ? Configurator.CONFIG.getInt("experiments.NumSource") : -1;
    
    m_srcEq.retainAll(m_testDict.getAllKeys());
    
    if ((numToKeep >= 0) && (m_srcEq.size() > numToKeep))
    {
      LinkedList<EquivalenceClass> valList = new LinkedList<EquivalenceClass>(m_srcEq);
      m_srcEq.clear();

      for (int i = 0; i < numToKeep; i++)
      {
        m_srcEq.add(valList.remove(m_rand.nextInt(valList.size())));
      }
    }
    
    LOG.info("Looking for translations for " + m_srcEq.size() + " random test dictionary source classes (see " + fileName + ").");
    
    StringBuilder strBld = new StringBuilder();
    
    for (EquivalenceClass eq : m_srcEq)
    { strBld.append(eq.toString() + "\t" + ((Number)eq.getProperty(Number.class.getName())).getNumber() + "\n");
    }
    
    LOG.info(strBld.toString());
    
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    writer.write(strBld.toString());
    writer.close();
  }
  
  protected void pruneContextsAccordingToScore(DictScorer scorer)
  {
    ScoreComparator comparator = new ScoreComparator(scorer);
    int pruneContextEqs = Configurator.CONFIG.getInt("experiments.context.PruneContextToSize");

    // Prune context
    for (EquivalenceClass ec : m_srcEq)
    { ((Context)ec.getProperty(Context.class.getName())).pruneContext(pruneContextEqs, comparator);
    }
    
    for (EquivalenceClass ec : m_trgEq)
    { ((Context)ec.getProperty(Context.class.getName())).pruneContext(pruneContextEqs, comparator);
    }
  }
  
  protected void scoreContexts(DictScorer scorer)
  {
    LOG.info("Scoring contextual items with " + scorer.toString() + "...");

    // Score contextual items
    for (EquivalenceClass ec : m_srcEq)
    { ((Context)ec.getProperty(Context.class.getName())).scoreContextualItems(scorer);
    }
    
    for (EquivalenceClass ec : m_trgEq)
    { ((Context)ec.getProperty(Context.class.getName())).scoreContextualItems(scorer);
    }
  }
  
  protected void dictCoverage(Dictionary dict, Set<EquivalenceClass> eqs, boolean source)
  {
    Set<EquivalenceClass> dictEntries = source ? dict.getAllKeys() : dict.getAllVals();  
    DecimalFormat df = new DecimalFormat("0.00");
    double tokTotal = 0;
    double tokCovered = 0;
    double typCovered = 0;
    Number numProp;
    double num;
      
    for (EquivalenceClass eq : eqs)
    { 
      if (null != (numProp = ((Number)eq.getProperty(Number.class.getName()))))
      {  
        num = numProp.getNumber();
      
        if (dictEntries.contains(eq))
        {
          tokCovered += num;
          typCovered++;
        }
        
        tokTotal += num;
      }
    }    
   
    LOG.info("[" + dict.getName() + (source ? "]: source" : "]: target") + " dictionary coverage " + df.format(100.0 * tokCovered / tokTotal) + "% tokens and " + df.format(100.0 * typCovered / (double)eqs.size()) + "% types.");
  }
  
  protected void collectTokenCounts(Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs)
  {
    m_maxTokCountInSrc = 0;
    m_maxTokCountInTrg = 0;
    m_numToksInSrc = 0;
    m_numToksInTrg = 0;
    
    Number tmpNum;
    
    for (EquivalenceClass eq : srcEqs)
    {
      if ((tmpNum = (Number)eq.getProperty(Number.class.getName())) != null)
      {
        if (tmpNum.getNumber() > m_maxTokCountInSrc)
        { m_maxTokCountInSrc = tmpNum.getNumber();
        }
        
        m_numToksInSrc += tmpNum.getNumber();
      }
    }

    for (EquivalenceClass eq : trgEqs)
    {
      if ((tmpNum = (Number)eq.getProperty(Number.class.getName())) != null) 
      {
        if (tmpNum.getNumber() > m_maxTokCountInTrg)
        { m_maxTokCountInTrg = tmpNum.getNumber();
        }
        
        m_numToksInTrg += tmpNum.getNumber();
      }
    }
    
    LOG.info("Maximum occurrences: src = " + m_maxTokCountInSrc + ", trg = " + m_maxTokCountInTrg + ".");
    LOG.info("Total Counts: src = " + m_numToksInSrc + ", trg = " + m_numToksInTrg + ".");    
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
  protected Dictionary m_seedDict;
  protected Dictionary m_useDict;
  protected Dictionary m_testDict;
  protected Set<EquivalenceClass> m_srcEq;
  protected Set<EquivalenceClass> m_trgEq;
  protected Set<EquivalenceClass> m_srcContextEq;
  protected Set<EquivalenceClass> m_trgContextEq;
  protected double m_numToksInSrc;
  protected double m_numToksInTrg;  
  protected double m_maxTokCountInSrc;
  protected double m_maxTokCountInTrg;
  
  public static class CountComparator implements Comparator<EquivalenceClass>
  {
    public int compare(EquivalenceClass item1, EquivalenceClass item2)
    {
      double diff = ((Number)item2.getProperty(Number.class.getName())).getNumber() - ((Number)item1.getProperty(Number.class.getName())).getNumber();
      
      return (diff == 0) ? 0 : (diff < 0) ? -1 : 1;
    }
  }
}