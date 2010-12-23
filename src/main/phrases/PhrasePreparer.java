package main.phrases;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.corpora.accessors.CrawlCorpusAccessor;
import babel.content.corpora.accessors.EuroParlCorpusAccessor;
import babel.content.corpora.accessors.LexCorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.content.eqclasses.collectors.EquivalenceClassCollector;
import babel.content.eqclasses.collectors.SimpleEquivalenceClassCollector;
import babel.content.eqclasses.comparators.LexComparator;
import babel.content.eqclasses.filters.DictionaryFilter;
import babel.content.eqclasses.filters.EquivalenceClassFilter;
import babel.content.eqclasses.filters.GarbageFilter;
import babel.content.eqclasses.filters.LengthFilter;
import babel.content.eqclasses.filters.NumOccurencesFilter;
import babel.content.eqclasses.filters.RomanizationFilter;
import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.NumberCollector;
import babel.content.eqclasses.properties.PhraseContext;
import babel.content.eqclasses.properties.PhraseNumberCollector;
import babel.content.eqclasses.properties.PhraseContextCollector;
import babel.content.eqclasses.properties.PhraseOrderCollector;
import babel.content.eqclasses.properties.PhraseTimeDistributionCollector;
import babel.content.eqclasses.properties.TimeDistribution;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Type.EqType;
import babel.ranking.scorers.Scorer;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.SimpleDictionary;
import babel.util.dict.SimpleDictionary.DictHalves;

public class PhrasePreparer {

  protected static final Log LOG = LogFactory.getLog(PhrasePreparer.class);

  public PhraseTable getPhraseTable() { 
    return m_phraseTable;
  }
  
  public Dictionary getSeedDict() {
    return m_seedDict;
  }

  public SimpleDictionary getTranslitDict() {
    return m_translitDict;
  }
  
  public long getNumSrcToks() { 
    return m_numToksInSrc;
  }
  
  public long getNumTrgToks() {
    return m_numToksInTrg;
  }
  
  public long getMaxSrcTokCount(){
    return m_maxTokCountInSrc;
  }
  
  public long getMaxTrgTokCount() {
    return m_maxTokCountInTrg; 
  }
  
  public long getMaxSrcPhrCount(){
    return m_maxPhrCountInSrc;
  }
  
  public long getMaxTrgPhrCount() {
    return m_maxPhrCountInTrg; 
  }
  
  public void clearPhraseTableFeatures(Set<Phrase> phrases) {

    LOG.info(" - Removing context and time phrase properties for " + phrases.size() + " phrases ...");

    if (phrases != null) {
      for (Phrase phrase : phrases) {
        phrase.removeProperty(TimeDistribution.class.getName());
        phrase.removeProperty(Context.class.getName());
      }
    }    
  }
  
  public void clearReorderingFeatures(Set<Phrase> phrases) {

    LOG.info(" - Removing ordering phrase properties for " + phrases.size() + " phrases ...");

    if (phrases != null) {
      for (Phrase phrase : phrases) {
        phrase.removeProperty(PhraseContext.class.getName());
      }
    }
  }
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected int readPhraseTableChunk(int chunkSize, boolean verbose) throws Exception {

    if (m_phraseTable == null) {
      boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");     
      m_phraseTable = new PhraseTable(caseSensitive);
    }
    
    if (verbose) {
      LOG.info(" - Reading candidate phrases from the phrase table...");
    }
    
    String phraseTableFile = Configurator.CONFIG.getString("resources.phrases.PhraseTable");
    int numRead = m_phraseTable.processPhraseTableFile(phraseTableFile, chunkSize);

    m_srcPhrs = (Set)m_phraseTable.getAllSrcPhrases();
    m_trgPhrs = (Set)m_phraseTable.getAllTrgPhrases();
    
    if (numRead == 0) {
      m_phraseTable.closePhraseTableFile();
      if (verbose) {
        LOG.info(" - Read an empty chunk - done processing phrase table.");
      }
    } else if (verbose) {
      LOG.info(" - Source phrases: " + m_srcPhrs.size());
      LOG.info(" - Target phrases: " + m_trgPhrs.size()); 
    }
    
    return numRead;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  protected void readPhrases(boolean readFromMono) throws Exception {
        
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    String phraseTableFile;
    
    if (readFromMono) {
      phraseTableFile = Configurator.CONFIG.getString("output.Path") +  "/" + Configurator.CONFIG.getString("output.MonoPhraseTable");
    } else {
      phraseTableFile = Configurator.CONFIG.getString("resources.phrases.PhraseTable");
    }
   
    LOG.info(" - Reading candidate phrases from the " + (readFromMono ? "mono" : "") + " phrase table (" + phraseTableFile + ") ...");
    
    m_phraseTable = new PhraseTable(phraseTableFile, -1, caseSensitive);
    
    m_srcPhrs = (Set)m_phraseTable.getAllSrcPhrases();
    m_trgPhrs = (Set)m_phraseTable.getAllTrgPhrases();
        
    LOG.info(" - Source phrases: " + m_srcPhrs.size());
    LOG.info(" - Target phrases: " + m_trgPhrs.size());   
  }
  
  protected synchronized void collectNumberProps(Set<Phrase> srcPhrs, Set<Phrase> trgPhrs, boolean computePhraseCounts, boolean verbose) throws Exception{

    LOG.info(" - Collecting phrase counts...");

    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
        
    // Start up the worker threads      
    NumberCollectorWorker srcNumberWorker = new NumberCollectorWorker(true, srcPhrs, maxPhraseLength, caseSensitive);
    NumberCollectorWorker trgNumberWorker = new NumberCollectorWorker(false, trgPhrs, maxPhraseLength, caseSensitive);
    
    /*
    
    m_runningThreads.clear();
    m_runningThreads.add(srcNumberWorker);
    m_runningThreads.add(trgNumberWorker);
    
    (new Thread(srcNumberWorker, "Source NumberCollectorWorker")).start();
    (new Thread(trgNumberWorker, "Target NumberCollectorWorker")).start();
   
    // Wait until both threads are done
    wait();
    
    */
    
    srcNumberWorker.run();
    trgNumberWorker.run();
    
    
    if (!srcNumberWorker.succeeded() || !trgNumberWorker.succeeded()) {
      throw new Exception("One of the number collecting threads failed.");
    }
    
    if (computePhraseCounts) {
      m_maxPhrCountInSrc = srcNumberWorker.getMaxPhrCount();

      m_maxPhrCountInTrg = trgNumberWorker.getMaxPhrCount();
    
      if (verbose) {
        LOG.info(" - Source phrases max occurrences = " + m_maxPhrCountInSrc);
        LOG.info(" - Target phrases max occurrences = " + m_maxPhrCountInTrg);
      }
    }
  }
  
  protected void collectTypeProp(Set<Phrase> srcPhrs, Set<Phrase> trgPhrs) {
    
    LOG.info(" - Assigning type phrase properties...");
    
    assignTypeProp(srcPhrs, EqType.SOURCE);
    assignTypeProp(trgPhrs, EqType.TARGET);
  }
 
  protected synchronized void collectOrderProps(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception{
    
    LOG.info(" - Collecting phrase ordering properties for " + srcPhrases.size() + " source and " + trgPhrases.size() + " target phrases " + " ...");
    
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    double keepContPhraseProb = Configurator.CONFIG.containsKey("preprocessing.phrases.ContPhraseKeepProb") ? Configurator.CONFIG.getDouble("preprocessing.phrases.ContPhraseKeepProb") : 1.0;  
    
    if (keepContPhraseProb == 1.0) {
      LOG.warn(" - Keeping ALL contextual phrases (reordering stage may take a loooong time)");
    }
    
    // Start up the worker threads
    PhraseOrderCollectorWorker srcOrderWorker = new PhraseOrderCollectorWorker(true, srcPhrases, maxPhraseLength, m_maxPhrCountInSrc, caseSensitive, m_srcPhrs, keepContPhraseProb);
    PhraseOrderCollectorWorker trgOrderWorker = new PhraseOrderCollectorWorker(false, trgPhrases, maxPhraseLength, m_maxPhrCountInTrg, caseSensitive, m_trgPhrs, keepContPhraseProb);
    
    /*
    
    m_runningThreads.clear();
    m_runningThreads.add(srcOrderWorker);
    m_runningThreads.add(trgOrderWorker);
    
    (new Thread(srcOrderWorker, "Source PhraseOrderCollectorWorker")).start();
    (new Thread(trgOrderWorker, "Target PhraseOrderCollectorWorker")).start();
   
    // Wait until both threads are done
    wait();
 
    */
    
    srcOrderWorker.run();
    trgOrderWorker.run();
    
    if (!srcOrderWorker.succeeded() || !trgOrderWorker.succeeded()) {
      throw new Exception("One of the order collecting threads failed.");
    }
  }

  protected synchronized void collectOtherProps(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception{
    
    LOG.info(" - Collecting context and time phrase properties for " + srcPhrases.size() + " source and " + trgPhrases.size() + " target phrases " + " ...");
    
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    int contextWindowSize = Configurator.CONFIG.getInt("preprocessing.context.Window");
    boolean alignDistros = Configurator.CONFIG.getBoolean("preprocessing.time.Align");

    // Start up the worker threads
    PropCollectorWorker srcPropWorker = new PropCollectorWorker(true, srcPhrases, maxPhraseLength, m_contextSrcEqs, contextWindowSize, caseSensitive);
    PropCollectorWorker trgPropWorker = new PropCollectorWorker(false, trgPhrases, maxPhraseLength, m_contextTrgEqs, contextWindowSize, caseSensitive);

    /*
    
    m_runningThreads.clear();
    m_runningThreads.add(srcPropWorker);
    m_runningThreads.add(trgPropWorker);
    
    (new Thread(srcPropWorker, "Source PropCollectorWorker")).start();
    (new Thread(trgPropWorker, "Target PropCollectorWorker")).start();
   
    // Wait until both threads are done
    wait();
    
    */
    
    srcPropWorker.run();
    trgPropWorker.run();
    
    if (!srcPropWorker.succeeded() || !trgPropWorker.succeeded()) {
      throw new Exception("One of the property collecting threads failed.");
    } else if (alignDistros) {
      LOG.info(" - Aligning temporal distributions...");
      alignDistributions(srcPropWorker.getBins(), trgPropWorker.getBins(), srcPhrases, trgPhrases);  
    }
  }
  
  @SuppressWarnings("unchecked")
  protected synchronized void collectContextEqs() throws Exception {
    
    LOG.info(" - Constructing contextual equivalence classes...");
    
    boolean filterRomanSrc = Configurator.CONFIG.containsKey("preprocessing.FilterRomanSrc") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanSrc");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    Class<EquivalenceClass> srcContClassClass = (Class<EquivalenceClass>)Class.forName(Configurator.CONFIG.getString("preprocessing.context.SrcEqClass"));
    Class<EquivalenceClass> trgContClassClass = (Class<EquivalenceClass>)Class.forName(Configurator.CONFIG.getString("preprocessing.context.TrgEqClass"));
    
    // Start up the worker threads
    ContextEqsCollectorWorker srcContextWorker = new ContextEqsCollectorWorker(true, true, filterRomanSrc, srcContClassClass);
    ContextEqsCollectorWorker trgContextWorker = new ContextEqsCollectorWorker(false, true, filterRomanTrg, trgContClassClass);

    
    /*

    m_runningThreads.clear();
    m_runningThreads.add(srcContextWorker);
    m_runningThreads.add(trgContextWorker);
    
    (new Thread(srcContextWorker, "Source ContextEqsCollectorWorker")).start();
    (new Thread(trgContextWorker, "Target ContextEqsCollectorWorker")).start();
   
    // Wait until both threads are done
    wait();
    
    */
    
    srcContextWorker.run();
    trgContextWorker.run();
    
    
    if (!srcContextWorker.succeeded() || !trgContextWorker.succeeded()) {
      throw new Exception("One of the property collecting threads failed.");
    }

    m_contextSrcEqs = srcContextWorker.getEqs();
    m_maxTokCountInSrc = srcContextWorker.getMaxTokCount();
    m_numToksInSrc = srcContextWorker.getNumToks();

    m_contextTrgEqs = trgContextWorker.getEqs();
    m_maxTokCountInTrg = trgContextWorker.getMaxTokCount();
    m_numToksInTrg = trgContextWorker.getNumToks();
    
    LOG.info(" - Source context classes = " + m_contextSrcEqs.size() + ", max occurrences = " + m_maxTokCountInSrc + ", total counts = " + m_numToksInSrc);
    LOG.info(" - Target context classes = " + m_contextTrgEqs.size() + ", max occurrences = " + m_maxTokCountInTrg + ", total counts = " + m_numToksInTrg);
  }
  
  protected void filterContextEqs() throws Exception {
  
    int pruneContEqIfOccursFewerThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursFewerThan");
    int pruneContEqIfOccursMoreThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursMoreThan");
    
    m_contextSrcEqs = filterContextEqs(true, m_contextSrcEqs, pruneContEqIfOccursFewerThan, pruneContEqIfOccursMoreThan);
    m_contextTrgEqs = filterContextEqs(false, m_contextTrgEqs, pruneContEqIfOccursFewerThan, pruneContEqIfOccursMoreThan);
  }
  
  protected Set<EquivalenceClass> filterContextEqs(boolean src, Set<EquivalenceClass> eqs, int pruneContEqIfOccursFewerThan, int pruneContEqIfOccursMoreThan) throws Exception {

    LOG.info(" - Filtering " + (src ? "source" : "target") + " contextual words: keeping those in dict [" + m_seedDict.toString() + "] and occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times...");
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new DictionaryFilter(m_seedDict, true, src)); 
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));

    Set<EquivalenceClass> filtContextEqs = EquivalenceClassCollector.filter(eqs, filters);
    LOG.info(" - Filtered context " + (src ? "source" : "target") + " classes: " + filtContextEqs.size());
    
    return filtContextEqs;
  }
  
  // Prepare for mono feature and order collection
  
  public void prepareForFeaturesAndOrderCollection() throws Exception {

    LOG.info(" - Preparing phrases...");
    readPhrases(false);
    collectNumberProps(m_srcPhrs, m_trgPhrs, true, true);  
    collectTypeProp(m_srcPhrs, m_trgPhrs); 

    collectContextEqs();
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    prepareTranslitDictionary(m_contextSrcEqs);
    filterContextEqs();

    collectOtherProps(m_srcPhrs, m_trgPhrs);
    collectOrderProps(m_srcPhrs, m_trgPhrs);
  }
  
  // Prepare for mono feature collection only
  /*
  public void preparePhrasesForFeaturesOnly() throws Exception {

    LOG.info(" - Preparing phrases for estimating monolingual features only ...");
    readPhrases(false);
    collectNumberProps(m_srcPhrs, m_trgPhrs);
    collectTypeProp(m_srcPhrs, m_trgPhrs);

    collectContextEqs();
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    prepareTranslitDictionary(m_contextSrcEqs);
    filterContextEqs();
    
    collectOtherProps(m_srcPhrs, m_trgPhrs);
  }
  */
  
  public void prepareForChunkFeaturesCollection() throws Exception {
    
    LOG.info(" - Preparing phrases for estimating monolingual features only ...");
    
    readPhrases(false);
    collectNumberProps(m_srcPhrs, m_trgPhrs, true, true);
    collectTypeProp(m_srcPhrs, m_trgPhrs); 
    
    collectContextEqs();
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    prepareTranslitDictionary(m_contextSrcEqs);
    filterContextEqs();
  
    m_srcPhrasesToProcess = new ArrayList<Phrase>(m_srcPhrs);
    Collections.sort(m_srcPhrasesToProcess, new LexComparator(true));
  }
  
  public void collectPropsForFeaturesOnly(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception {    
    collectOtherProps(srcPhrases, trgPhrases); 
  }
  
  public void prepareForChunkFeaturesCollectionForAnni(int chunkSize) throws Exception {
    
    LOG.info(" - Preparing phrases for estimating monolingual features only ...");
    
    // Go though phrase table in chunks and compute max occur numbers
    long curMaxInSrc = 0, curMaxInTrg = 0;
    m_maxPhrCountInSrc = m_maxPhrCountInTrg = 0;
    
    while (readPhraseTableChunk(chunkSize, false) > 0) {
      
      collectNumberProps(m_srcPhrs, m_trgPhrs, true, false);

      if (m_maxPhrCountInSrc > curMaxInSrc) {
        curMaxInSrc = m_maxPhrCountInSrc;
      }
      
      if (m_maxPhrCountInTrg > curMaxInTrg) {
        curMaxInTrg = m_maxPhrCountInTrg;
      }
    }

    m_maxPhrCountInSrc = curMaxInSrc;
    m_maxPhrCountInTrg = curMaxInTrg;
    
    m_phraseTable = null;
    m_srcPhrs = null;
    m_trgPhrs = null;
    
    LOG.info(" - Source phrases max occurrences = " + m_maxPhrCountInSrc);
    LOG.info(" - Target phrases max occurrences = " + m_maxPhrCountInTrg);
     
    collectContextEqs();
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    prepareTranslitDictionary(m_contextSrcEqs);
    filterContextEqs();
  }
  
  public int readNextChunkForAnni(int chunkSize, int chunkNum) throws Exception {
    
    LOG.info(" - Reading chunk " + chunkNum + " of phrase table ...");

    int numRead;
    
    if ((numRead = readPhraseTableChunk(chunkSize, true)) > 0) {
      
      collectNumberProps(m_srcPhrs, m_trgPhrs, false, false);
      collectTypeProp(m_srcPhrs, m_trgPhrs); 
      
      m_srcPhrasesToProcess = new ArrayList<Phrase>(m_srcPhrs);
      Collections.sort(m_srcPhrasesToProcess, new LexComparator(true));
    } else {
      m_srcPhrasesToProcess = new ArrayList<Phrase>();
    }
        
    return numRead;
  }
  
  
  
  // Prepare for ordering collection only
  /*
  public void preparePhrasesForOrderingOnly() throws Exception {

    LOG.info(" - Preparing phrases for estimating ordering features only ...");
    readPhrases(true);
    collectNumberProps(m_srcPhrs, m_trgPhrs);  
    collectTypeProp(m_srcPhrs, m_trgPhrs);
    
    collectOrderProps(m_srcPhrs, m_trgPhrs);
  }
  */
  
  public void prepareForChunkOrderCollection()  throws Exception {

    LOG.info(" - Preparing phrases for estimating ordering features only ...");
    readPhrases(true);
    collectNumberProps(m_srcPhrs, m_trgPhrs, true, true);  
    collectTypeProp(m_srcPhrs, m_trgPhrs);  
    
    m_srcPhrasesToProcess = new ArrayList<Phrase>(m_srcPhrs);
    Collections.sort(m_srcPhrasesToProcess, new LexComparator(true));
  }
  
  public void collectPropsForOrderOnly(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception {    
    collectOrderProps(srcPhrases, trgPhrases); 
  }
  
  public Set<Phrase> getNextChunk(int chunkSize) {
    
    Set<Phrase> chunk = null;  
  
    if (chunkSize > 0 && m_srcPhrasesToProcess != null && m_srcPhrasesToProcess.size() > 0) {
      
      int maxIdx = Math.min(chunkSize, m_srcPhrasesToProcess.size());
      chunk = new HashSet<Phrase>(m_srcPhrasesToProcess.subList(0, maxIdx));
    
      if (maxIdx < chunkSize) {
        m_srcPhrasesToProcess.clear();
      } else {
        m_srcPhrasesToProcess = m_srcPhrasesToProcess.subList(maxIdx, m_srcPhrasesToProcess.size());
      }
    }
      
    return chunk;
  }
  
  protected void prepareSeedDictionary(Set<EquivalenceClass> srcContEqs, Set<EquivalenceClass> trgContEqs) throws Exception {
    
    String dictDir = Configurator.CONFIG.getString("resources.dictionary.Path");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
    SimpleDictionary simpSeedDict;
    
    LOG.info(" - Reading/preparing seed dictionary ...");
    
    if (Configurator.CONFIG.containsKey("resources.dictionary.Dictionary")) {
      String dictFileName = Configurator.CONFIG.getString("resources.dictionary.Dictionary");
      simpSeedDict = new SimpleDictionary(dictDir + dictFileName, "SeedDictionary");
    } else {
      String srcDictFileName = Configurator.CONFIG.getString("resources.dictionary.SrcName");
      String trgDictFileName = Configurator.CONFIG.getString("resources.dictionary.TrgName");      
      simpSeedDict = new SimpleDictionary(new DictHalves(dictDir + srcDictFileName, dictDir + trgDictFileName) , "SeedDictionary");
    }

    simpSeedDict.pruneCounts(ridDictNumTrans);
    
    m_seedDict = new Dictionary(srcContEqs, trgContEqs, simpSeedDict, "SeedDictionary");
    
    LOG.info(" - Seed dictionary: " + m_seedDict.toString()); 
  }

  protected void prepareTranslitDictionary(Set<EquivalenceClass> srcContEqs) throws Exception {
        
    LOG.info(" - Reading/preparing transliteration dictionary ...");
    
    String dictDir = Configurator.CONFIG.containsKey("resources.translit.Path") ? Configurator.CONFIG.getString("resources.translit.Path") : null;

    if ((dictDir == null) || (dictDir.trim().length() == 0)) {
      
      LOG.info(" - No transliteration dictionary specified");
      
    } else {
      
      if (Configurator.CONFIG.containsKey("resources.translit.Dictionary")) {
        String dictFileName = Configurator.CONFIG.getString("resources.translit.Dictionary");
        m_translitDict = new SimpleDictionary(dictDir + dictFileName, "Translit");
      } else {
        String srcDictFileName = Configurator.CONFIG.getString("resources.translit.SrcName");
        String trgDictFileName = Configurator.CONFIG.getString("resources.translit.TrgName");      
        m_translitDict = new SimpleDictionary(new DictHalves(dictDir + srcDictFileName, dictDir + trgDictFileName) , "TranslitDictionary");
      }
        
      LOG.info(" - Transliteration dictionary: " + m_translitDict.toString()); 
    }
  }

  protected void alignDistributions(Set<Integer> srcBins, Set<Integer> trgBins, Set<Phrase> srcEqs, Set<Phrase> trgEqs)
  {
    HashSet<Integer> toRemove = new HashSet<Integer>(srcBins);
    TimeDistribution timeProp;
    toRemove.removeAll(trgBins);
    
    for (EquivalenceClass eq : srcEqs)
    {
      if (null != (timeProp = (TimeDistribution)eq.getProperty(TimeDistribution.class.getName())))
      { timeProp.removeBins(toRemove);
      }
    }
    
    toRemove.clear();
    toRemove.addAll(trgBins);
    toRemove.removeAll(srcBins);
    
    for (EquivalenceClass eq : trgEqs)
    {
      if (null != (timeProp = (TimeDistribution)eq.getProperty(TimeDistribution.class.getName())))
      { timeProp.removeBins(toRemove);
      }
    }       
    
    toRemove.clear();
    toRemove.addAll(srcBins);
    toRemove.retainAll(trgBins);
    
    LOG.info("There are " + srcBins.size() + " days in src distributions."); 
    LOG.info("There are " + trgBins.size() + " days in trg distributions."); 
    LOG.info("There are " + toRemove.size() + " common days between src and trg distributions.");    
  }
  
  protected Set<EquivalenceClass> constructEqClasses(boolean src, Set<EquivalenceClass> allEqs, Class<? extends EquivalenceClass> eqClassClass) throws Exception
  {    
    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>();
    EquivalenceClass newEq, foundEq;
    String newWord;
    long newCount;
        
    for (EquivalenceClass eq : allEqs)
    {
      newWord = ((SimpleEquivalenceClass)eq).getWord(); // TODO: not pretty
      newCount = ((Number)eq.getProperty(Number.class.getName())).getNumber();
      
      newEq = eqClassClass.newInstance();
      newEq.init(newWord, true);
      
      if (null == (foundEq = eqsMap.get(newEq.getStem())))
      {
        newEq.assignId();
        newEq.setProperty(new Number(newCount));
        newEq.setProperty(new Type(src ? EqType.SOURCE : EqType.TARGET));
        
        eqsMap.put(newEq.getStem(), newEq);
      }
      else
      {
        foundEq.merge(newEq);
 
        ((Number)foundEq.getProperty(Number.class.getName())).increment(newCount);
      }
    }
    
    return new HashSet<EquivalenceClass>(eqsMap.values());
  }
    
  // Returns a tuple with counts (0 -> maximum count, 1 -> total count)
  protected long[] collectCounts(Set<? extends EquivalenceClass> eqs) {

    long[] c = new long[2];
    c[0] = c[1] = 0;
    
    Number num;
    TimeDistribution distro;
    long count;
    
    for (EquivalenceClass eq : eqs)
    {
      count = -1;
      
      if ((num = (Number)eq.getProperty(Number.class.getName())) != null) {
        count = num.getNumber();
      } else if ((distro = (TimeDistribution)eq.getProperty(TimeDistribution.class.getName())) != null) {
        count = distro.getTotalOccurences();
      }
      
      if (count > 0) {
        
        if (count > c[0])
        { c[0] = count;
        }
        
        c[1] += count;
      }
    }

    return c;
  }
  
  protected void assignTypeProp(Set<? extends EquivalenceClass> eqClasses, EqType type)
  {
    Type commonType = new Type(type);
    
    for (EquivalenceClass eq : eqClasses)
    { eq.setProperty(commonType);
    }
  }
  
  public void prepareProperties(boolean src, Set<? extends EquivalenceClass> eqs, Scorer contextScorer, Scorer timeScorer)
  {
    LOG.info(" - Projecting and scoring " + (src ? "source" : "target") + " contextual items with " + contextScorer.toString() + " and time distributions with " + timeScorer.toString() + "...");

    for (EquivalenceClass eq : eqs)
    { 
      contextScorer.prepare(eq);
      timeScorer.prepare(eq);
    }
  }

  public void pruneMostFrequentContext(boolean src, Set<? extends EquivalenceClass> phrases, int numKeepBefore, int numKeepAfter, int numKeepDisc) {
      
    LOG.info(" - Pruning context for " + (src ? "source" : "target") + " phrases ...");
    
    PhraseContext context;
    int bBefore = 0, bAfter = 0, bDisc = 0;
    int aBefore = 0, aAfter = 0, aDisc = 0;
    
    for (EquivalenceClass phrase : phrases) {      
      
      if (null != (context = ((PhraseContext)phrase.getProperty(PhraseContext.class.getName())))) {
        bBefore += context.getBefore().size();
        bAfter += context.getAfter().size();
        bDisc += context.getDiscontinuous().size();
        
        context.pruneMostFreq(numKeepBefore, numKeepAfter, numKeepDisc);
        aBefore += context.getBefore().size();
        aAfter += context.getAfter().size();
        aDisc += context.getDiscontinuous().size();
      }
    }

    LOG.info(" - Pruned context: before " + bBefore + "->" + aBefore + ", after " + bAfter + "->" + aAfter + ", discontinous " + bDisc + "->" + aDisc);
  }
  
  protected synchronized CorpusAccessor getAccessor(String kind, boolean src) throws Exception
  {
    CorpusAccessor accessor = null;

    if ("europarl".equals(kind))
    { accessor = getEuroParlAccessor(src);
    }
    else if ("wiki".equals(kind))
    { accessor = getWikiAccessor(src);
    }
    else if ("crawls".equals(kind))
    { accessor = getCrawlsAccessor(src);
    }
    else if ("dev".equals(kind))
    { accessor = getDevAccessor(src);
    }
    else if ("test".equals(kind))
    { accessor = getTestAccessor(src);
    }
    else
    { LOG.error("Could not find corpus accessor for " + kind);
    }
    
    return accessor;
  }

  protected LexCorpusAccessor getDevAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.dev.Path");
    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.dev.OneSentPerLine");
    String name = src ? Configurator.CONFIG.getString("corpora.dev.SrcName") : Configurator.CONFIG.getString("corpora.dev.TrgName");
        
    return new LexCorpusAccessor(name, appendSep(path), oneSentPerLine);    
  }
  
  protected LexCorpusAccessor getTestAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.test.Path");
    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.test.OneSentPerLine");
    String name = src ? Configurator.CONFIG.getString("corpora.test.SrcName") : Configurator.CONFIG.getString("corpora.test.TrgName");
        
    return new LexCorpusAccessor(name, appendSep(path), oneSentPerLine);    
  }
  
  protected EuroParlCorpusAccessor getEuroParlAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.europarl.Path");
    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.europarl.OneSentPerLine");
    String subDir = src ? Configurator.CONFIG.getString("corpora.europarl.SrcSubDir") : Configurator.CONFIG.getString("corpora.europarl.TrgSubDir");

    SimpleDateFormat sdf = new SimpleDateFormat( "yy-MM-dd" );
    Date fromDate = sdf.parse(Configurator.CONFIG.getString("corpora.europarl.DateFrom"));
    Date toDate = sdf.parse(Configurator.CONFIG.getString("corpora.europarl.DateTo"));
    
    return new EuroParlCorpusAccessor(appendSep(path) + subDir, fromDate, toDate, oneSentPerLine);
  }
  
  protected CrawlCorpusAccessor getCrawlsAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.crawls.Path");
    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.crawls.OneSentPerLine");
    String subDir = src ? Configurator.CONFIG.getString("corpora.crawls.SrcSubDir") : Configurator.CONFIG.getString("corpora.crawls.TrgSubDir");

    SimpleDateFormat sdf = new SimpleDateFormat( "yy-MM-dd" );
    Date fromDate = sdf.parse(Configurator.CONFIG.getString("corpora.crawls.DateFrom"));
    Date toDate = sdf.parse(Configurator.CONFIG.getString("corpora.crawls.DateTo"));
    
    return new CrawlCorpusAccessor(appendSep(path) + subDir, fromDate, toDate, oneSentPerLine);
  }
  
  protected LexCorpusAccessor getWikiAccessor(boolean src)
  {
    String path = Configurator.CONFIG.getString("corpora.wiki.Path");
    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.wiki.OneSentPerLine");
    String fileRegExp = src ? Configurator.CONFIG.getString("corpora.wiki.SrcRegExp") : Configurator.CONFIG.getString("corpora.wiki.TrgRegExp");
  
    return new LexCorpusAccessor(fileRegExp, appendSep(path), oneSentPerLine);
  }

  protected String appendSep(String str)
  {
    String ret = (str == null) ? null : str.trim();
    
    if (ret != null && ret.length() > 0 && !ret.endsWith(File.separator))
    { ret += File.separator; 
    }
    
    return ret;
  }
  
  protected synchronized void workerDone(Runnable worker) {
    
    m_runningThreads.remove(worker);
    
    if (m_runningThreads.size() == 0) {
      notify();
    }
  }
  
  protected HashSet<Runnable> m_runningThreads = new HashSet<Runnable>();

  protected PhraseTable m_phraseTable;
  
  protected Dictionary m_seedDict = null;
  protected SimpleDictionary m_translitDict = null;
  
  protected Set<EquivalenceClass> m_contextSrcEqs = null;
  protected Set<EquivalenceClass> m_contextTrgEqs = null;
  protected Set<Phrase> m_srcPhrs = null;
  protected Set<Phrase> m_trgPhrs = null;
  
  protected List<Phrase> m_srcPhrasesToProcess = null;
  
  protected long m_numToksInSrc = 0;
  protected long m_numToksInTrg = 0;
  protected long m_maxTokCountInSrc = 0;
  protected long m_maxTokCountInTrg = 0;
  protected long m_maxPhrCountInSrc = 0;
  protected long m_maxPhrCountInTrg = 0;

  class PropCollectorWorker implements Runnable {
    public PropCollectorWorker(boolean src, Set<Phrase> phrases, int maxPhraseLength, Set<EquivalenceClass> contextEqs, int contextWindowSize, boolean caseSensitive) {
      
      m_succeeded = true;
      m_src = src;
      m_phrases = phrases;
      m_contextEqs = contextEqs;
      m_maxPhraseLength = maxPhraseLength;
      m_caseSensitive = caseSensitive;
      m_contextWindowSize = contextWindowSize;
      m_bins = null;
    }

    public void run() {

      try {

        CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), m_src);    
        (new PhraseContextCollector(m_maxPhraseLength, m_caseSensitive, m_contextWindowSize, m_contextWindowSize, m_contextEqs)).collectProperty(accessor, m_phrases);

        // Collect time properties
        accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Time"), m_src);
        PhraseTimeDistributionCollector distCollector = new PhraseTimeDistributionCollector(m_maxPhraseLength, m_caseSensitive);
        distCollector.collectProperty(accessor, m_phrases);
        
        m_bins = distCollector.binsCollected();

      } catch (Exception e) {        
        LOG.error(e.toString());
        m_succeeded = false;
      }
      
      PhrasePreparer.this.workerDone(this);
    }
    
    // Returns time bins for which counts were collected
    public Set<Integer> getBins() {
      return m_bins;
    }
    
    public boolean succeeded() {
      return m_succeeded;
    }

    boolean m_succeeded;
    boolean m_src;
    Set<Phrase> m_phrases;
    int m_maxPhraseLength;
    Set<EquivalenceClass> m_contextEqs;
    int m_contextWindowSize;
    boolean m_caseSensitive; 
    Set<Integer> m_bins;
  }
  
  class NumberCollectorWorker implements Runnable {
    public NumberCollectorWorker(boolean src, Set<Phrase> phrases, int maxPhraseLength, boolean caseSensitive) {
      
      m_src = src;
      m_phrases = phrases;
      m_maxPhraseLength = maxPhraseLength;
      m_caseSensitive = caseSensitive;
      m_maxPhrCount = 0;
      m_numPhrs = 0;
      m_succeeded = true;
    }

    public void run() {
            
      try {
        // Collect counts and order properties
        CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), m_src);
        (new PhraseNumberCollector(m_maxPhraseLength, m_caseSensitive)).collectProperty(accessor, m_phrases);

        long[] c = collectCounts(m_phrases); 
        
        m_maxPhrCount = c[0];
        m_numPhrs = c[1];  

      } catch (Exception e) {
        LOG.error(e.toString());
        m_succeeded = false;
      }
      
      PhrasePreparer.this.workerDone(this);
    }
    
    public long getMaxPhrCount() {
      return m_maxPhrCount;
    }
    
    public long getNumPhrs() {
      return m_numPhrs;
    }
    
    public boolean succeeded() {
      return m_succeeded;
    }
    
    boolean m_succeeded;
    Set<Phrase> m_phrases;
    boolean m_src;
    int m_maxPhraseLength;
    boolean m_caseSensitive; 
    long m_maxPhrCount;
    long m_numPhrs;
  }
  
  class PhraseOrderCollectorWorker implements Runnable {
    public PhraseOrderCollectorWorker(boolean src, Set<Phrase> phrases, int maxPhraseLength, long maxPhraseCountInCorpus, boolean caseSensitive, Set<Phrase> allPhrases, double keepContPhraseProb) {
      
      m_src = src;
      m_phrases = phrases;
      m_maxPhraseLength = maxPhraseLength;
      m_maxPhraseCountInCorpus = maxPhraseCountInCorpus;
      m_caseSensitive = caseSensitive;
      m_allPhrases = allPhrases;
      m_keepContPhraseProb = keepContPhraseProb;
      m_succeeded = true;
    }

    public void run() {

      try {  
        CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), m_src);
        (new PhraseOrderCollector(m_src, m_maxPhraseLength, m_caseSensitive, m_maxPhraseCountInCorpus, m_allPhrases, m_keepContPhraseProb)).collectProperty(accessor, m_phrases);
        
      } catch (Exception e) {
        LOG.error(e.toString());
        m_succeeded = false;
      }
      
      PhrasePreparer.this.workerDone(this);
    }
    
    public boolean succeeded() {
      return m_succeeded;
    }
    
    boolean m_succeeded;
    Set<Phrase> m_phrases;
    boolean m_src;
    int m_maxPhraseLength;
    long m_maxPhraseCountInCorpus;
    boolean m_caseSensitive;
    Set<Phrase> m_allPhrases;
    double m_keepContPhraseProb;
  }
  
  class ContextEqsCollectorWorker implements Runnable {
    public ContextEqsCollectorWorker(boolean src, boolean caseSensitive, boolean filterRoman, Class<EquivalenceClass> contextClassClass) {
      m_src = src;
      m_caseSensitive = caseSensitive;
      m_succeeded = true;
      m_filterRoman = filterRoman;
      m_eqs = null;
      m_contextClassClass = contextClassClass;
    }

    public void run() {

      try {          
        ArrayList<EquivalenceClassFilter> filters = new ArrayList<EquivalenceClassFilter>(3);
        filters.add(new GarbageFilter());
        filters.add(new LengthFilter(2));
        
        if (m_filterRoman)
        { filters.add(new RomanizationFilter());
        }
        
        CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), m_src);
        
        // Collect init classes
        SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, m_caseSensitive);
        m_eqs = collector.collect(accessor.getCorpusReader(), -1);

        // TODO: break up into multiple workers?
        
        // Collect counts property
        (new NumberCollector(m_caseSensitive)).collectProperty(accessor, m_eqs);

        // Construct context classes
        m_eqs = constructEqClasses(m_src, m_eqs, m_contextClassClass);
        
        // Assign type property
        assignTypeProp(m_eqs, m_src ? EqType.SOURCE : EqType.TARGET);
        
        long[] c = collectCounts(m_eqs);
        m_maxTokCount = c[0];
        m_numToks = c[1];
        
      } catch (Exception e) {
        LOG.error(e.toString());
        m_succeeded = false;
      }
      
      PhrasePreparer.this.workerDone(this);
    }
    
    public boolean succeeded() {
      return m_succeeded;
    }
    
    public Set<EquivalenceClass> getEqs() {
      return m_eqs;
    }
    
    public long getMaxTokCount() {
      return m_maxTokCount;
    }

    public long getNumToks() {
      return m_numToks;
    }
    
    boolean m_succeeded;
    Set<EquivalenceClass> m_eqs;
    boolean m_src;
    boolean m_caseSensitive; 
    boolean m_filterRoman;
    long m_maxTokCount;
    long m_numToks;
    Class<EquivalenceClass> m_contextClassClass;
  } 
 }
