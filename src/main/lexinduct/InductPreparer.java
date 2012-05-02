package main.lexinduct;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
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
import babel.content.eqclasses.filters.DictionaryFilter;
import babel.content.eqclasses.filters.EquivalenceClassFilter;
import babel.content.eqclasses.filters.GarbageFilter;
import babel.content.eqclasses.filters.LengthFilter;
import babel.content.eqclasses.filters.NoContextFilter;
import babel.content.eqclasses.filters.NoTimeDistributionFilter;
import babel.content.eqclasses.filters.NumOccurencesFilter;
import babel.content.eqclasses.filters.RomanizationFilter;
import babel.content.eqclasses.properties.context.ContextCollector;
import babel.content.eqclasses.properties.number.Number;
import babel.content.eqclasses.properties.number.NumberCollector;
import babel.content.eqclasses.properties.time.TimeDistribution;
import babel.content.eqclasses.properties.time.TimeDistributionCollector;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.ranking.scorers.Scorer;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.SimpleDictionary;
import babel.util.dict.SimpleDictionary.DictHalves;

public class InductPreparer {
  
  protected static final Log LOG = LogFactory.getLog(InductPreparer.class);
  protected static final String DEFAULT_ENCODING = "UTF-8";
  
  public Dictionary getSeedDict() {
    return m_seedDict;
  }
  
  public double getNumSrcToks() { 
    return m_numToksInSrc;
  }
  
  public double getNumTrgToks() {
    return m_numToksInTrg;
  }
  
  public double getMaxSrcTokCount(){
    return m_maxTokCountInSrc;
  }
  
  public double getMaxTrgTokCount() {
    return m_maxTokCountInTrg; 
  }
  
  public Set<EquivalenceClass> getSrcEqsToInduct()
  { return m_srcEqs;
  }
  
  public Set<EquivalenceClass> getTrgEqs()
  { return m_trgEqs;
  }
  
  @SuppressWarnings({ "unchecked" })
  public void prepare() throws Exception {

    boolean filterRomanSrc = Configurator.CONFIG.containsKey("preprocessing.FilterRomanSrc") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanSrc");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    String srcContEqClassName = Configurator.CONFIG.getString("preprocessing.context.SrcEqClass");
    String trgContEqClassName = Configurator.CONFIG.getString("preprocessing.context.TrgEqClass");
    boolean alignDistros = Configurator.CONFIG.getBoolean("preprocessing.time.Align");
    
    Class<EquivalenceClass> srcContClassClass = (Class<EquivalenceClass>)Class.forName(srcContEqClassName);
    Class<EquivalenceClass> trgContClassClass = (Class<EquivalenceClass>)Class.forName(trgContEqClassName);
    
    LOG.info(" - Collecting from scratch ...");
    
    Set<EquivalenceClass> allSrcEqs = collectInitEqClasses(true, filterRomanSrc);
    Set<EquivalenceClass> allTrgEqs = collectInitEqClasses(false, filterRomanTrg);
    LOG.info(" - All source types: " + allSrcEqs.size() + (filterRomanSrc ? " (without romanization) " : ""));
    LOG.info(" - All target types: " + allTrgEqs.size() + (filterRomanTrg ? " (without romanization) " : ""));
    
    LOG.info(" - Constructing context classes...");
    m_contextSrcEqs = constructEqClasses(true, allSrcEqs, srcContClassClass);
    m_contextTrgEqs = constructEqClasses(false, allTrgEqs, trgContClassClass);     
    LOG.info(" - Context source classes: " + m_contextSrcEqs.size());
    LOG.info(" - Context target classes: " + m_contextTrgEqs.size());
    
    //LOG.info(" - Writing context classes...");
    //writeEqs(m_contextSrcEqs, true, CONTEXT_SRC_MAP_FILE, CONTEXT_SRC_PROP_EXT);
    //writeEqs(m_contextTrgEqs, false, CONTEXT_TRG_MAP_FILE, CONTEXT_TRG_PROP_EXT);
    
    LOG.info(" - Reading source and target candidates...");    
    m_srcEqs = prepareCandidates(true, m_contextSrcEqs);
    m_trgEqs = prepareCandidates(false, m_contextTrgEqs);
     
    LOG.info(" - Source candidates: " + m_srcEqs.size());
    LOG.info(" - Target candidates: " + m_trgEqs.size());
    
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);

    LOG.info(" - Collecting properties...");
    Set<Integer> srcBins = collectProps(true, m_srcEqs, m_contextSrcEqs, m_seedDict);
    Set<Integer> trgBins = collectProps(false, m_trgEqs, m_contextTrgEqs, m_seedDict);

    if (alignDistros)
    {
      LOG.info(" - Aligning temporal distributions...");
      alignDistributions(srcBins, trgBins, m_srcEqs, m_trgEqs);
    }
    
    LOG.info(" - Cleaning up candidate classes...");
    m_srcEqs = cleanUpEqClasses(m_srcEqs, true);
    m_trgEqs = cleanUpEqClasses(m_trgEqs, false); 
    LOG.info(" - Source candidates: " + m_srcEqs.size());
    LOG.info(" - Target candidates: " + m_trgEqs.size());
    
    //LOG.info(" - Writing candidate classes and properties...");
    //writeEqs(m_srcEqs, true, SRC_MAP_FILE, SRC_PROP_EXT);
    //writeProps(m_srcEqs, true, SRC_PROP_EXT);
    //writeEqs(m_trgEqs, false, TRG_MAP_FILE, TRG_PROP_EXT);
    //writeProps(m_trgEqs, false, TRG_PROP_EXT);
    
    collectTokenCounts(m_contextSrcEqs, m_contextTrgEqs);
  }

  protected HashSet<EquivalenceClass> prepareCandidates(boolean src, Set<EquivalenceClass> contEqs) throws Exception {
    
    String dictDir = Configurator.CONFIG.getString("resources.inductvocab.Path");
    String fileName = src ? Configurator.CONFIG.getString("resources.inductvocab.SrcName") : Configurator.CONFIG.getString("resources.inductvocab.TrgName");    
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(dictDir + fileName), DEFAULT_ENCODING));
    String line;
    EquivalenceClass foundEq;
    HashSet<EquivalenceClass> eqSet = new HashSet<EquivalenceClass>();
    HashMap<String, EquivalenceClass> eqMap = new HashMap<String, EquivalenceClass>();
    
    LOG.info("Reading " + (src ? "source" : "target") + " candidates ...");
    
    for (EquivalenceClass eq : contEqs) {
      for (String sWord : eq.getAllWords()) { 
        assert !eqMap.containsKey(sWord);
        eqMap.put(sWord, eq);
      }
    }
    
    while (null != (line = reader.readLine()))
    {
      foundEq = eqMap.get(line.split("\\s")[0]);

      if (foundEq != null) {
        eqSet.add(foundEq);
      }      
    }
    
    reader.close();
    
    return eqSet;
  }
  
  protected Set<EquivalenceClass> cleanUpEqClasses(Set<EquivalenceClass> eqClasses, boolean src) throws Exception
  {
    LOG.info("Throwing out " + (src ? "source" : "target") + " candidate classes without context or time properties..."); 

    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new NoContextFilter());
    filters.add(new NoTimeDistributionFilter());
    
    return EquivalenceClassCollector.filter(eqClasses, filters);    
  }
  
  protected void prepareSeedDictionary(Set<EquivalenceClass> srcContEqs, Set<EquivalenceClass> trgContEqs) throws Exception {
    
    String dictDir = Configurator.CONFIG.getString("resources.dictionary.Path");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
    SimpleDictionary simpSeedDict;
    
    LOG.info("Reading/preparing seed dictionary ...");
    
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
    
    LOG.info("Seed dictionary: " + m_seedDict.toString()); 
  }
  
  protected void alignDistributions(Set<Integer> srcBins, Set<Integer> trgBins, Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs)
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
  
  protected Set<EquivalenceClass> collectInitEqClasses(boolean src, boolean filterRoman) throws Exception
  {
    Set<EquivalenceClass> eqClasses;
  
    ArrayList<EquivalenceClassFilter> filters = new ArrayList<EquivalenceClassFilter>(3);
    filters.add(new GarbageFilter());
    filters.add(new LengthFilter(2));
    if (filterRoman)
    { filters.add(new RomanizationFilter());
    }
    
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    
    // Collect init classes
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, true);
    eqClasses = collector.collect(accessor.getCorpusReader(), -1);

    // Collect counts property
    (new NumberCollector(true)).collectProperty(accessor, eqClasses);

    // Assign type property
    assignTypeProp(eqClasses, src ? EqType.SOURCE : EqType.TARGET);
    
    return eqClasses;
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
      newEq.init(newWord, false);
      
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
  
  protected Set<Integer> collectProps(boolean src, Set<EquivalenceClass> eqClasses, Set<EquivalenceClass> contextEqs, Dictionary contextDict) throws Exception
  {
    int pruneContEqIfOccursFewerThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursFewerThan");
    int pruneContEqIfOccursMoreThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursMoreThan");
    int contextWindowSize = Configurator.CONFIG.getInt("preprocessing.context.Window");
    
    Set<EquivalenceClass> filtContextEqs = new HashSet<EquivalenceClass>(contextEqs);

    LOG.info("Preparing contextual words for " + (src ? "source" : "target") + ": keeping those in dict [" + contextDict.toString() + "] and occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times...");
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new DictionaryFilter(contextDict, true, src)); 
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    filtContextEqs = EquivalenceClassCollector.filter(filtContextEqs, filters);
    LOG.info("Context " + (src ? "source" : "target") + " classes: " + filtContextEqs.size());
    
    // Collect context properties
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);    
    (new ContextCollector(true, false, contextWindowSize, contextWindowSize, contextEqs)).collectProperty(accessor, eqClasses); 
    
    // Collect time properties
    accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Time"), src);
    TimeDistributionCollector distCollector = new TimeDistributionCollector(true);
    distCollector.collectProperty(accessor, eqClasses);
    
    // Assign type property
    assignTypeProp(eqClasses, src ? EqType.SOURCE : EqType.TARGET);
    
    // Returns time bins for which counts were collected
    return distCollector.binsCollected();
  }
  
  protected void collectTokenCounts(Set<? extends EquivalenceClass> srcEqs, Set<? extends EquivalenceClass> trgEqs)
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
  
  protected void assignTypeProp(Set<? extends EquivalenceClass> eqClasses, EqType type)
  {
    Type commonType = new Type(type);
    
    for (EquivalenceClass eq : eqClasses)
    { eq.setProperty(commonType);
    }
  }
  
  public void prepareProperties(boolean src, Set<? extends EquivalenceClass> eqs, Scorer contextScorer, Scorer timeScorer)
  {
    LOG.info("Projecting and scoring " + (src ? "source" : "target") + " contextual items with " + contextScorer.toString() + " and time distributions with " + timeScorer.toString() + "...");

    for (EquivalenceClass eq : eqs)
    { 
      contextScorer.prepare(eq);
      timeScorer.prepare(eq);
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
  
  protected Dictionary m_seedDict = null;
  
  protected Set<EquivalenceClass> m_contextSrcEqs = null;
  protected Set<EquivalenceClass> m_contextTrgEqs = null;
  protected Set<EquivalenceClass> m_srcEqs = null;
  protected Set<EquivalenceClass> m_trgEqs = null;
  
  protected double m_numToksInSrc = 0;
  protected double m_numToksInTrg = 0;
  protected double m_maxTokCountInSrc = 0;
  protected double m_maxTokCountInTrg = 0;
}
