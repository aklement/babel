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
import babel.content.corpora.accessors.WikiTempCorpusAccessor;

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
import babel.content.eqclasses.properties.context.Context;
import babel.content.eqclasses.properties.context.PhraseContextCollector;
import babel.content.eqclasses.properties.number.Number;
import babel.content.eqclasses.properties.number.NumberCollector;
import babel.content.eqclasses.properties.number.PhraseNumberCollector;
import babel.content.eqclasses.properties.order.PhraseContext;
import babel.content.eqclasses.properties.order.PhraseOrderCollector;
import babel.content.eqclasses.properties.time.PhraseTimeDistributionCollector;
import babel.content.eqclasses.properties.time.TimeDistribution;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.content.eqclasses.properties.lshcontext.LSHContextCollector;
import babel.content.eqclasses.properties.lshtime.LSHTimeDistributionCollector;
import babel.content.eqclasses.properties.lshorder.LSHPhraseContextCollector;

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
      phraseTableFile = Configurator.CONFIG.getString("output.Path") +  "/" + Configurator.CONFIG.getString("output.PhraseTablePL");
    } else {
      phraseTableFile = Configurator.CONFIG.getString("resources.phrases.PhraseTable");
    }
   
    LOG.info(" - Reading candidate phrases from the" + (readFromMono ? " mono " : " ") + "phrase table (" + phraseTableFile + ") ...");
    
    m_phraseTable = new PhraseTable(phraseTableFile, -1, caseSensitive);
    
    m_srcPhrs = (Set)m_phraseTable.getAllSrcPhrases();
    m_trgPhrs = (Set)m_phraseTable.getAllTrgPhrases();
        
    LOG.info(" - Source phrases: " + m_srcPhrs.size());
    LOG.info(" - Target phrases: " + m_trgPhrs.size());   
  }
  
  protected void collectNumberProps(Set<Phrase> srcPhrs, Set<Phrase> trgPhrs, boolean computePhraseCounts, boolean verbose) throws Exception {

    LOG.info(" - Collecting phrase counts...");

    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    
    collectNumberProps(true, srcPhrs, maxPhraseLength, caseSensitive);
    collectNumberProps(false, trgPhrs, maxPhraseLength, caseSensitive);
    
    if (computePhraseCounts) {
      
      m_maxPhrCountInSrc = collectMaxOccurrenceCount(srcPhrs);
      m_maxPhrCountInTrg = collectMaxOccurrenceCount(trgPhrs);
    
      if (verbose) {
        LOG.info(" - Source phrases max occurrences = " + m_maxPhrCountInSrc);
        LOG.info(" - Target phrases max occurrences = " + m_maxPhrCountInTrg);
      }
    }
  }
  
  /**
   * @return max count of any of the given phrases in the corpus
   */
  protected void collectNumberProps(boolean src, Set<Phrase> phrases, int maxPhraseLength, boolean caseSensitive) throws Exception {
    // Collect counts
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    (new PhraseNumberCollector(maxPhraseLength, caseSensitive)).collectProperty(accessor, phrases);
  }
  
  protected void collectTypeProp(Set<Phrase> srcPhrs, Set<Phrase> trgPhrs) {
    
    LOG.info(" - Assigning type phrase properties...");
    
    assignTypeProp(srcPhrs, EqType.SOURCE);
    assignTypeProp(trgPhrs, EqType.TARGET);
  }
 
  protected void collectOrderProps(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception{
    
    LOG.info(" - Collecting phrase ordering properties for " + srcPhrases.size() + " source and " + trgPhrases.size() + " target phrases " + " ...");
    
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    int maxToksBetween = Configurator.CONFIG.getInt("preprocessing.phrases.MaxToksBetween");
    boolean collectLongestOnly = Configurator.CONFIG.getBoolean("preprocessing.phrases.CollectLongestOnly");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    double keepContPhraseProb = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.ContPhraseKeepProb") ? Configurator.CONFIG.getDouble("preprocessing.phrases.reordering.ContPhraseKeepProb") : 1.0;  
    
    if (keepContPhraseProb == 1.0) {
      LOG.warn(" - Keeping ALL contextual phrases at collection");
    }
    
    collectOrderProps(true, srcPhrases, maxPhraseLength, maxToksBetween, collectLongestOnly, m_maxPhrCountInSrc, caseSensitive, m_srcPhrs, keepContPhraseProb);
    collectOrderProps(false, trgPhrases, maxPhraseLength, maxToksBetween, collectLongestOnly, m_maxPhrCountInTrg, caseSensitive, m_trgPhrs, keepContPhraseProb);
  }
  
  protected void collectOrderProps(boolean src, Set<Phrase> phrases, int maxPhraseLength, int maxToksBetween, boolean collectLongestOnly, long maxPhraseCountInCorpus, boolean caseSensitive, Set<Phrase> allPhrases, double keepContPhraseProb) throws Exception { 
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    (new PhraseOrderCollector(src, maxPhraseLength, maxToksBetween, collectLongestOnly, caseSensitive, maxPhraseCountInCorpus, allPhrases, keepContPhraseProb)).collectProperty(accessor, phrases);
  }

  protected void collectContextAndTimeProps(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception{
    
    LOG.info(" - Collecting context and time phrase properties for " + srcPhrases.size() + " source and " + trgPhrases.size() + " target phrases " + " ...");
    
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    int contextWindowSize = Configurator.CONFIG.getInt("preprocessing.context.Window");
    boolean alignDistros = Configurator.CONFIG.getBoolean("preprocessing.time.Align");

    Set<Integer> srcBins = collectContextAndTimeProps(true, srcPhrases, maxPhraseLength, m_contextSrcEqs, contextWindowSize, caseSensitive);
    Set<Integer> trgBins = collectContextAndTimeProps(false, trgPhrases, maxPhraseLength, m_contextTrgEqs, contextWindowSize, caseSensitive);
    
    if (alignDistros) {
      LOG.info(" - Aligning temporal distributions...");
      alignDistributions(srcBins, trgBins, srcPhrases, trgPhrases);  
    }
  }
  

  /***
   * @return Check that filenames in two wikitemp lists are the same
   */
  public boolean checkWikiTemp() throws Exception{
	  WikiTempCorpusAccessor srcAccessor = getWikiTempAccessor(true);
	  WikiTempCorpusAccessor trgAccessor = getWikiTempAccessor(false);
	  
	  String[] srcfiles = srcAccessor.getFileList().getFileNames();
	  String[] trgfiles = trgAccessor.getFileList().getFileNames();
	  
	  String[] srcfilesFix = new String[srcfiles.length];
	  int i=0;
	  for (String s: srcfiles){
		  srcfilesFix[i]=s.substring(0,((s.length())-3));
		  i++;
	  }

	  String[] trgfilesFix = new String[trgfiles.length];
	  i=0;
	  for (String s: trgfiles){
		  trgfilesFix[i]=s.substring(0,((s.length())-3));
		  i++;
	  }

	  if (srcfilesFix.length!=trgfilesFix.length){
		  return false;
	  }

	  
	  i=0;
	  while (i<srcfilesFix.length){
		  if (!srcfilesFix[i].equals(trgfilesFix[i])){
			  
			  System.out.println("SOURCE FILES:"+srcfilesFix[i]);
			  System.out.println("TARGET FILES:"+trgfilesFix[i]);
			  return false;
		  }
		  i++;
	  }
	  
	  return true;
	  
  }

  
  /**
   * @return time bins for which counts were collected
   */
  protected Set<Integer> collectContextAndTimeProps(boolean src, Set<Phrase> phrases, int maxPhraseLength, Set<EquivalenceClass> contextEqs, int contextWindowSize, boolean caseSensitive) throws Exception {

    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);    
    (new PhraseContextCollector(maxPhraseLength, caseSensitive, contextWindowSize, contextWindowSize, contextEqs)).collectProperty(accessor, phrases);

    accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Time"), src);
    PhraseTimeDistributionCollector distCollector = new PhraseTimeDistributionCollector(maxPhraseLength, caseSensitive);
    distCollector.collectProperty(accessor, phrases);
      
    return distCollector.binsCollected();
  }
  
  @SuppressWarnings("unchecked")
  protected void collectContextEqs() throws Exception {
    
    LOG.info(" - Constructing contextual equivalence classes...");
    
    boolean filterRomanSrc = Configurator.CONFIG.containsKey("preprocessing.FilterRomanSrc") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanSrc");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    Class<EquivalenceClass> srcContClassClass = (Class<EquivalenceClass>)Class.forName(Configurator.CONFIG.getString("preprocessing.context.SrcEqClass"));
    Class<EquivalenceClass> trgContClassClass = (Class<EquivalenceClass>)Class.forName(Configurator.CONFIG.getString("preprocessing.context.TrgEqClass"));
    
    m_contextSrcEqs = collectContextEqs(true, true, filterRomanSrc, srcContClassClass);
    m_contextTrgEqs = collectContextEqs(false, true, filterRomanTrg, trgContClassClass);
   
    m_maxTokCountInSrc = collectMaxOccurrenceCount(m_contextSrcEqs);
    m_maxTokCountInTrg = collectMaxOccurrenceCount(m_contextTrgEqs);
    
    LOG.info(" - Source context classes = " + m_contextSrcEqs.size() + ", max occurrences = " + m_maxTokCountInSrc);
    LOG.info(" - Target context classes = " + m_contextTrgEqs.size() + ", max occurrences = " + m_maxTokCountInTrg);
  }
 
  protected Set<EquivalenceClass> collectContextEqs(boolean src, boolean caseSensitive, boolean filterRoman, Class<EquivalenceClass> contextClassClass) throws Exception {
    
    Set<EquivalenceClass> eqs;
    ArrayList<EquivalenceClassFilter> filters = new ArrayList<EquivalenceClassFilter>(3);
    filters.add(new GarbageFilter());
    filters.add(new LengthFilter(2));
    
    if (filterRoman) {
      filters.add(new RomanizationFilter());
    }
    
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    
    // Collect init classes
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, caseSensitive);
    eqs = collector.collect(accessor.getCorpusReader(), -1);
    // Collect counts property
    (new NumberCollector(caseSensitive)).collectProperty(accessor, eqs);
    // Construct context classes
    eqs = constructEqClasses(src, eqs, contextClassClass);
    // Assign type property
    assignTypeProp(eqs, src ? EqType.SOURCE : EqType.TARGET);
        
    return eqs;
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

    collectContextAndTimeProps(m_srcPhrs, m_trgPhrs);
    collectOrderProps(m_srcPhrs, m_trgPhrs);
  }
  
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
    collectContextAndTimeProps(srcPhrases, trgPhrases); 
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
 
  protected long collectMaxOccurrenceCount(Set<? extends EquivalenceClass> eqs) {

    long maxOccurCount = 0;
    
    Number num;
    long count;
    
    for (EquivalenceClass eq : eqs) {
      
      if ((num = (Number)eq.getProperty(Number.class.getName())) != null) {

        if ((count = num.getNumber()) > maxOccurCount)
        { maxOccurCount = count;
        }        
      }
    }

    return maxOccurCount;
  }
  
  protected void assignTypeProp(Set<? extends EquivalenceClass> eqClasses, EqType type) {
    
    Type commonType = new Type(type);
    
    for (EquivalenceClass eq : eqClasses)
    { eq.setProperty(commonType);
    }
  }
  
  public void prepareContextAndTimeProps(boolean src, Set<? extends EquivalenceClass> eqs, Scorer contextScorer, Scorer timeScorer, boolean mapToLSH) throws Exception {
    
    LOG.info(" - " + (src ? "Scoring source" : "Projecting and scoring target") + " contextual items with " + contextScorer.toString() + " and time distributions with " + timeScorer.toString() + "...");
    for (EquivalenceClass eq : eqs) { 
      contextScorer.prepare(eq);
      timeScorer.prepare(eq);
    }

    if (mapToLSH) {
      LOG.info(" - Mapping " + (src ? "source" : "target") + " context into LSH space...");    
      (new LSHContextCollector(true)).collectProperty(eqs);
      LOG.info(" - Mapping " + (src ? "source" : "target") + " temporal into LSH space...");    
      (new LSHTimeDistributionCollector(true)).collectProperty(eqs);

      //String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");
      //String ext = src ? ".src" : ".trg";
      //EqClassPersister.persistProperty(eqs, LSHContext.class.getName(), preProcDir + LSHContext.class.getSimpleName() + ext);
      //EqClassPersister.persistProperty(eqs, LSHTimeDistribution.class.getName(), preProcDir + LSHTimeDistribution.class.getSimpleName() + ext);
    }
  }
  
  public void prepareOrderProps(boolean src, Set<? extends EquivalenceClass> eqs, boolean mapToLSH) throws Exception {
    if (mapToLSH) {
      LOG.info(" - Mapping " + (src ? "source" : "target") + " ordering vectors into LSH space...");    
      (new LSHPhraseContextCollector(true, m_phraseTable)).collectProperty(eqs);
    }
  }

  public void pruneMostFrequentContext(boolean src, Set<? extends EquivalenceClass> phrases) {
    int numKeepBefore;
    int numKeepAfter;
    int numKeepDisc;
 
    if (src) {
      numKeepBefore = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.SrcContPhraseKeepBefore") ? Configurator.CONFIG.getInt("preprocessing.phrases.reordering.SrcContPhraseKeepBefore") : -1;  
      numKeepAfter = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.SrcContPhraseKeepAfter") ? Configurator.CONFIG.getInt("preprocessing.phrases.reordering.SrcContPhraseKeepAfter") : -1;  
      numKeepDisc = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.SrcContPhraseKeepDisc") ? Configurator.CONFIG.getInt("preprocessing.phrases.reordering.SrcContPhraseKeepDisc") : -1;
    } else {
      numKeepBefore = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.TrgContPhraseKeepBefore") ? Configurator.CONFIG.getInt("preprocessing.phrases.reordering.TrgContPhraseKeepBefore") : -1;  
      numKeepAfter = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.TrgContPhraseKeepAfter") ? Configurator.CONFIG.getInt("preprocessing.phrases.reordering.TrgContPhraseKeepAfter") : -1;  
      numKeepDisc = Configurator.CONFIG.containsKey("preprocessing.phrases.reordering.TrgContPhraseKeepDisc") ? Configurator.CONFIG.getInt("preprocessing.phrases.reordering.TrgContPhraseKeepDisc") : -1;  
    }
    
    LOG.info(" - Pruning context for " + (src ? "source" : "target") + " phrases. Keeping most frequent " + numKeepBefore + " before, " + numKeepAfter + " after, and " + numKeepDisc + " discontinous phrases...");
    
    PhraseContext context;
    long bBefore = 0, bAfter = 0, bDisc = 0;
    long aBefore = 0, aAfter = 0, aDisc = 0;
    
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
  
  protected CorpusAccessor getAccessor(String kind, boolean src) throws Exception
  {
    CorpusAccessor accessor = null;

    if ("europarl".equals(kind))
    { accessor = getEuroParlAccessor(src);
    }
    else if ("wiki".equals(kind))
    { accessor = getWikiAccessor(src);
    }
    else if ("wikitemp".equals(kind))
    { accessor = getWikiTempAccessor(src);
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
  
  protected WikiTempCorpusAccessor getWikiTempAccessor(boolean src){
	    String path = Configurator.CONFIG.getString("corpora.wikitemp.Path");
	    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.wikitemp.OneSentPerLine");
	    String fileRegExp = src ? Configurator.CONFIG.getString("corpora.wikitemp.SrcRegExp") : Configurator.CONFIG.getString("corpora.wikitemp.TrgRegExp");

	    return new WikiTempCorpusAccessor(fileRegExp, appendSep(path), oneSentPerLine);

  }

  protected String appendSep(String str)
  {
    String ret = (str == null) ? null : str.trim();
    
    if (ret != null && ret.length() > 0 && !ret.endsWith(File.separator))
    { ret += File.separator; 
    }
    
    return ret;
  }
  
  protected PhraseTable m_phraseTable;
  
  protected Dictionary m_seedDict = null;
  protected SimpleDictionary m_translitDict = null;
  
  protected Set<EquivalenceClass> m_contextSrcEqs = null;
  protected Set<EquivalenceClass> m_contextTrgEqs = null;
  protected Set<Phrase> m_srcPhrs = null;
  protected Set<Phrase> m_trgPhrs = null;
  
  protected List<Phrase> m_srcPhrasesToProcess = null;
  
  protected long m_maxTokCountInSrc = 0;
  protected long m_maxTokCountInTrg = 0;
  protected long m_maxPhrCountInSrc = 0;
  protected long m_maxPhrCountInTrg = 0;
  
 }
