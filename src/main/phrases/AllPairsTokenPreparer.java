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
import babel.content.eqclasses.phrases.PhraseSet;
import babel.content.eqclasses.phrases.PhraseTable;
import babel.content.eqclasses.properties.context.PhraseContextCollector;
import babel.content.eqclasses.properties.lshcontext.LSHContextCollector;
import babel.content.eqclasses.properties.lshtime.LSHTimeDistributionCollector;
import babel.content.eqclasses.properties.number.Number;
import babel.content.eqclasses.properties.number.NumberCollector;
import babel.content.eqclasses.properties.number.PhraseNumberCollector;
import babel.content.eqclasses.properties.time.PhraseTimeDistributionCollector;
import babel.content.eqclasses.properties.time.TimeDistribution;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.ranking.scorers.Scorer;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.SimpleDictionary;
import babel.util.dict.SimpleDictionary.DictHalves;

public class AllPairsTokenPreparer {

  protected static final Log LOG = LogFactory.getLog(AllPairsTokenPreparer.class);

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
  
  public Set<Phrase> getSrcPhrases() {
    return m_srcPhrs;
  }

  public Set<Phrase> getTrgPhrases() {
    return m_trgPhrs;
  }
  
  public void prepareForFeaturesCollection() throws Exception {
    
    LOG.info(" - Preparing phrases for estimating monolingual features ...");
    
    readPhrases();
    collectNumberProps(m_srcPhrs, m_trgPhrs, true);
    collectTypeProp(m_srcPhrs, m_trgPhrs); 
    
    collectContextEqs();
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    prepareTranslitDictionary(m_contextSrcEqs);
    filterContextEqs();
  
    m_srcPhrasesToProcess = new ArrayList<Phrase>(m_srcPhrs);
    Collections.sort(m_srcPhrasesToProcess, new LexComparator(true));
  }
  
  public PhraseTable getNextChunkToProcess(int chunkSrcSize) {

    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    int toIdx = Math.min(chunkSrcSize, m_srcPhrasesToProcess.size());
    PhraseTable phTable = null;
    
    if (toIdx > 0) {
      Set<Phrase> srcSet = new HashSet<Phrase>(m_srcPhrasesToProcess.subList(0, toIdx));
      m_srcPhrasesToProcess.removeAll(srcSet);
      phTable = new PhraseTable(srcSet, m_trgPhrs, caseSensitive);
    }
    
    return phTable;
  }
  
  public void collectContextAndTimeProps(Set<Phrase> srcPhrases, Set<Phrase> trgPhrases) throws Exception{
    
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
  
  protected void readPhrases() throws Exception {
        
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    String srcPhraseFile = Configurator.CONFIG.getString("resources.phrases.SrcPhraseFile");
    String trgPhraseFile = Configurator.CONFIG.getString("resources.phrases.TrgPhraseFile");
   
    LOG.info(" - Reading candidate src phrases from " + srcPhraseFile + " ...");
    m_srcPhrs = (new PhraseSet(srcPhraseFile,caseSensitive, 1)).getPhrases();
    
    LOG.info(" - Reading candidate trg phrases from " + trgPhraseFile + " ...");
    m_trgPhrs = (new PhraseSet(trgPhraseFile,caseSensitive, 1)).getPhrases();
        
    LOG.info(" - Source phrases: " + m_srcPhrs.size());
    LOG.info(" - Target phrases: " + m_trgPhrs.size());   
  }
  
  protected void collectNumberProps(Set<Phrase> srcPhrs, Set<Phrase> trgPhrs, boolean verbose) throws Exception {

    LOG.info(" - Collecting phrase counts...");

    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    
    collectNumberProps(true, srcPhrs, maxPhraseLength, caseSensitive);
    collectNumberProps(false, trgPhrs, maxPhraseLength, caseSensitive);
  }
  
  /**
   * @return max count of any of the given phrases in the corpus
   */
  protected void collectNumberProps(boolean src, Set<Phrase> phrases, int maxPhraseLength, boolean caseSensitive) throws Exception {
    // Collect counts
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    (new PhraseNumberCollector(maxPhraseLength, caseSensitive)).collectProperty(accessor, phrases);
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
  
  protected void collectTypeProp(Set<Phrase> srcPhrs, Set<Phrase> trgPhrs) {
    
    LOG.info(" - Assigning type phrase properties...");
    
    assignTypeProp(srcPhrs, EqType.SOURCE);
    assignTypeProp(trgPhrs, EqType.TARGET);
  }
  
  protected void assignTypeProp(Set<? extends EquivalenceClass> eqClasses, EqType type) {
    
    Type commonType = new Type(type);
    
    for (EquivalenceClass eq : eqClasses)
    { eq.setProperty(commonType);
    }
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
  
  protected Set<Phrase> m_srcPhrs = null;
  protected Set<Phrase> m_trgPhrs = null;
  
  protected long m_maxTokCountInSrc = 0;
  protected long m_maxTokCountInTrg = 0;

  protected Dictionary m_seedDict = null;
  protected SimpleDictionary m_translitDict = null;
  
  protected Set<EquivalenceClass> m_contextSrcEqs = null;
  protected Set<EquivalenceClass> m_contextTrgEqs = null;

  protected List<Phrase> m_srcPhrasesToProcess = null;
}
