package main;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
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
import babel.content.eqclasses.comparators.NumberComparator;
import babel.content.eqclasses.filters.DictionaryFilter;
import babel.content.eqclasses.filters.EquivalenceClassFilter;
import babel.content.eqclasses.filters.GarbageFilter;
import babel.content.eqclasses.filters.LengthFilter;
import babel.content.eqclasses.filters.NoContextFilter;
import babel.content.eqclasses.filters.NoTimeDistributionFilter;
import babel.content.eqclasses.filters.NumOccurencesFilter;
import babel.content.eqclasses.filters.RomanizationFilter;
import babel.content.eqclasses.filters.StopWordsFilter;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.ContextCollector;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.NumberCollector;
import babel.content.eqclasses.properties.TimeDistribution;
import babel.content.eqclasses.properties.TimeDistributionCollector;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Type.EqType;

import babel.ranking.scorers.Scorer;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.SimpleDictionary;
import babel.util.dict.SimpleDictionary.DictPair;
import babel.util.persistence.EqClassPersister;

public class DataPreparer
{
  protected static final Log LOG = LogFactory.getLog(DataPreparer.class);
  
  protected static final String INIT_SRC_MAP_FILE = "init.src.map";
  protected static final String INIT_TRG_MAP_FILE = "init.trg.map";

  protected static final String INIT_SRC_PROP_EXT = ".init.src.map";
  protected static final String INIT_TRG_PROP_EXT = ".init.trg.map";
  
  protected static final String SRC_MAP_FILE = "src.map";
  protected static final String TRG_MAP_FILE = "trg.map";
  
  protected static final String SRC_PROP_EXT = ".src.map";
  protected static final String TRG_PROP_EXT = ".trg.map";

  protected static final String SRC_TO_INDUCT = "srcinduct.list";
  
  @SuppressWarnings("unchecked")
  public void prepareEqs() throws Exception
  {
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    String srcClassName = Configurator.CONFIG.getString("preprocessing.SrcEqClass");
    String trgClassName = Configurator.CONFIG.getString("preprocessing.TrgEqClass");
    String srcStopFileName = Configurator.CONFIG.getString("resources.stopwords.SrcStopWords");
    String trgStopFileName = Configurator.CONFIG.getString("resources.stopwords.TrgStopWords");
    
    Class<EquivalenceClass> srcClassClass = (Class<EquivalenceClass>)Class.forName(srcClassName);
    Class<EquivalenceClass> trgClassClass = (Class<EquivalenceClass>)Class.forName(trgClassName);
    
    // Prepare equivalence classes and their properties
    try
    {
      LOG.info("Reading previously collected initial source classes from " + INIT_SRC_MAP_FILE + " and target from " + INIT_TRG_MAP_FILE + "...");
      m_allSrcEqs = readEqClasses(true, SimpleEquivalenceClass.class, INIT_SRC_MAP_FILE, INIT_SRC_PROP_EXT);
      m_allTrgEqs = readEqClasses(false, SimpleEquivalenceClass.class, INIT_TRG_MAP_FILE, INIT_TRG_PROP_EXT);
      LOG.info("All initial source classes: " + m_allSrcEqs.size());
      LOG.info("All initial target classes: " + m_allTrgEqs.size());
  
      
      LOG.info("Reading previously constructed source classes from " + SRC_MAP_FILE + " and target from " + TRG_MAP_FILE + "...");
      m_srcEqs = readEqClasses(true, srcClassClass, SRC_MAP_FILE, SRC_PROP_EXT);
      m_trgEqs = readEqClasses(false, trgClassClass, TRG_MAP_FILE, TRG_PROP_EXT);
      LOG.info("All source classes: " + m_srcEqs.size());
      LOG.info("All target classes: " + m_trgEqs.size());
      
      prepareDictionariesFromSingleFile(m_allSrcEqs, m_allTrgEqs, m_srcEqs, m_trgEqs);
      
      LOG.info("Reading source and target properties...");
      readProps(true, m_srcEqs, SRC_PROP_EXT);
      readProps(false, m_trgEqs, TRG_PROP_EXT);
    }
    catch(Exception e)
    { 
      LOG.info("Failed to read initial classes (" + e.toString() + "), collecting from scratch ...");
      m_allSrcEqs = collectInitEqClasses(true, INIT_SRC_MAP_FILE, INIT_SRC_PROP_EXT, false);
      m_allTrgEqs = collectInitEqClasses(false, INIT_TRG_MAP_FILE, INIT_TRG_PROP_EXT, filterRomanTrg);
      LOG.info("All initial source classes: " + m_allSrcEqs.size());
      LOG.info("All initial target classes: " + m_allTrgEqs.size() + (filterRomanTrg ? " (without romanization) " : ""));
      
      LOG.info("Constructing source and target classes...");
      m_srcEqs = constructEqClasses(true, m_allSrcEqs, srcClassClass, SRC_MAP_FILE, SRC_PROP_EXT);
      m_trgEqs = constructEqClasses(false, m_allTrgEqs, trgClassClass, TRG_MAP_FILE, TRG_PROP_EXT);
      LOG.info("All source classes: " + m_srcEqs.size());
      LOG.info("All target classes: " + m_trgEqs.size());
      
      prepareDictionariesFromSingleFile(m_allSrcEqs, m_allTrgEqs, m_srcEqs, m_trgEqs);
      
      LOG.info("Collecting source and target properties...");
      
      collectProps(true, m_srcEqs, m_allSrcEqs, m_seedDict, SRC_PROP_EXT);
      collectProps(false, m_trgEqs, m_allTrgEqs, m_seedDict, TRG_PROP_EXT);
    }
    
    // Measure dictionary coverage
    dictCoverage(m_seedDict, m_allSrcEqs, true);
    dictCoverage(m_seedDict, m_allTrgEqs, false);
   
    collectTokenCounts(m_allSrcEqs, m_allTrgEqs);
    
    // Prune source and target candidates 
    LOG.info("Pruning source and target equivalence classes...");
    m_srcEqs = (Set<EquivalenceClass>) pruneEqClasses(m_srcEqs, true, srcStopFileName, srcClassName, false);
    m_trgEqs = (Set<EquivalenceClass>) pruneEqClasses(m_trgEqs, false, trgStopFileName, trgClassName, filterRomanTrg);
    LOG.info("Pruned source classes: " + m_srcEqs.size());
    LOG.info("Pruned target classes: " + m_trgEqs.size());    
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
  
  public Dictionary getSeedDict()
  { return m_seedDict;
  }
  
  public Dictionary getTestDict() 
  { return m_testDict;
  }
  
  public Set<EquivalenceClass> getSrcEqs()
  { return m_srcEqs;
  }
  
  public Set<EquivalenceClass> getTrgEqs()
  { return m_trgEqs;
  }
  
  public double getNumSrcToks()
  { return m_numToksInSrc;
  }
  
  public double getNumTrgToks()
  { return m_numToksInTrg;
  }
  
  public double getMaxSrcTokCount()
  { return m_maxTokCountInSrc;
  }
  
  public double getMaxTrgTokCount()
  { return m_maxTokCountInTrg; 
  }
  
  protected Set<EquivalenceClass> readEqClasses(boolean src, Class<? extends EquivalenceClass> eqClsssClass, String eqfileName, String propFileExtension) throws Exception
  {
    // Read init classes
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");        
    Set<EquivalenceClass> eqClasses = EqClassPersister.unpersistEqClasses(eqClsssClass, preProcDir + eqfileName);
    
    // Read counts property
    EqClassPersister.unpersistProperty(eqClasses, Number.class.getName(), preProcDir + Number.class.getSimpleName() + propFileExtension);
    
    // Assign type property
    assignTypeProp(eqClasses, src ? EqType.SOURCE : EqType.TARGET);
    
    return eqClasses;
  }
  
  protected Set<EquivalenceClass> collectInitEqClasses(boolean src, String eqfileName, String propFileExtension, boolean filterRomanTrg) throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");
    Set<EquivalenceClass> eqClasses;
  
    ArrayList<EquivalenceClassFilter> filters = new ArrayList<EquivalenceClassFilter>(3);
    filters.add(new GarbageFilter());
    filters.add(new LengthFilter(2));
    if (filterRomanTrg)
    { filters.add(new RomanizationFilter());
    }
    
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    
    // Collect init classes
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, false);
    eqClasses = collector.collect(accessor.getCorpusReader(), -1);
    EqClassPersister.persistEqClasses(eqClasses, preProcDir + eqfileName);        

    // Collect counts property
    (new NumberCollector(false)).collectProperty(accessor, eqClasses);
    EqClassPersister.persistProperty(eqClasses, Number.class.getName(), preProcDir + Number.class.getSimpleName() + propFileExtension);

    // Assign type property
    assignTypeProp(eqClasses, src ? EqType.SOURCE : EqType.TARGET);
    
    return eqClasses;
  }

  protected Set<EquivalenceClass> constructEqClasses(boolean src, Set<EquivalenceClass> allEqs, Class<? extends EquivalenceClass> eqClassClass, String eqfileName, String propFileExtension) throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");
    
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
    
    HashSet<EquivalenceClass> eqClasses = new HashSet<EquivalenceClass>(eqsMap.values());
    
    EqClassPersister.persistEqClasses(eqClasses, preProcDir + eqfileName);        
    EqClassPersister.persistProperty(eqClasses, Number.class.getName(), preProcDir + Number.class.getSimpleName() + propFileExtension);
    
    return eqClasses;
  }
  
  protected void readProps(boolean src, Set<EquivalenceClass> eqClasses, String propFileExtension) throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");
    
    // Read properties
    EqClassPersister.unpersistProperty(eqClasses, Number.class.getName(), preProcDir + Number.class.getSimpleName() + propFileExtension); 
    EqClassPersister.unpersistProperty(eqClasses, Context.class.getName(), preProcDir + Context.class.getSimpleName() + propFileExtension);  
    EqClassPersister.unpersistProperty(eqClasses, TimeDistribution.class.getName(), preProcDir + TimeDistribution.class.getSimpleName() + propFileExtension);
    
    // Assign type property
    assignTypeProp(eqClasses, src ? EqType.SOURCE : EqType.TARGET);
  }
  
  protected void collectProps(boolean src, Set<EquivalenceClass> eqClasses, Set<EquivalenceClass> contextEqs, Dictionary contextDict, String propFileExtension) throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");  
    int pruneContEqIfOccursFewerThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursFewerThan");
    int pruneContEqIfOccursMoreThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursMoreThan");
    int contextWindowSize = Configurator.CONFIG.getInt("preprocessing.context.Window");
    int timeWindowSize = Configurator.CONFIG.getInt("preprocessing.time.Window");
    boolean slidingWindow = Configurator.CONFIG.getBoolean("preprocessing.time.SlidingWindow");
    
    Set<EquivalenceClass> filtContextEqs = new HashSet<EquivalenceClass>(contextEqs);

    LOG.info("Preparing contextual words for " + (src ? "source" : "target") + ": keeping those in dict [" + contextDict.toString() + "] and occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times...");
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new DictionaryFilter(contextDict, true, src)); 
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    filtContextEqs = EquivalenceClassCollector.filter(filtContextEqs, filters);
    LOG.info("Context " + (src ? "source" : "target") + " classes: " + filtContextEqs.size());
    
    // Collect properties
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);    
    (new ContextCollector(false, contextWindowSize, contextWindowSize, filtContextEqs)).collectProperty(accessor, eqClasses);    

    accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Time"), src);    
    (new TimeDistributionCollector(false, timeWindowSize, slidingWindow)).collectProperty(accessor, eqClasses);
    
    EqClassPersister.persistProperty(eqClasses, Context.class.getName(), preProcDir + Context.class.getSimpleName() + propFileExtension);
    EqClassPersister.persistProperty(eqClasses, TimeDistribution.class.getName(), preProcDir + TimeDistribution.class.getSimpleName() + propFileExtension);
    
    // Assign type property
    assignTypeProp(eqClasses, src ? EqType.SOURCE : EqType.TARGET);
  }

  protected Set<EquivalenceClass> pruneEqClasses(Set<EquivalenceClass> eqClasses, boolean src, String stopWordsFileName, String eqClassName, boolean filterRomanTrg) throws Exception
  {
    String stopWordsDir = Configurator.CONFIG.getString("resources.stopwords.Path");
    int pruneCandIfOccursFewerThan = Configurator.CONFIG.getInt("preprocessing.candidates.PruneIfOccursFewerThan");
    int pruneCandIfOccursMoreThan = Configurator.CONFIG.getInt("preprocessing.candidates.PruneIfOccursMoreThan");    

    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new GarbageFilter());
    if (filterRomanTrg)
    { filters.add(new RomanizationFilter());
    }
        
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, false);
    Set<? extends EquivalenceClass> stopEqs = ((new File(stopWordsDir + stopWordsFileName)).exists()) ?
      collector.collect((new LexCorpusAccessor(stopWordsFileName, stopWordsDir)).getCorpusReader(), -1) :
      new HashSet<EquivalenceClass>();
    
    LOG.info("Pruning " + (src ? "source" : "target")  + " candidate words: removing " + stopEqs.size()  + " stop words, eq classes with no context or distro, and keeping if occur (" + pruneCandIfOccursFewerThan + "," + pruneCandIfOccursMoreThan + ") times..."); 
        
    filters.clear();
    filters.add(new StopWordsFilter(stopEqs));
    filters.add(new NoContextFilter());
    filters.add(new NoTimeDistributionFilter());
    filters.add(new NumOccurencesFilter(pruneCandIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursMoreThan, false));
    
    return EquivalenceClassCollector.filter(eqClasses, filters);    
  }

  protected void dictCoverage(Dictionary dict, Set<EquivalenceClass> eqs, boolean src)
  {
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
      
        if ((src && dict.containsSrc(eq)) || (!src && dict.containsTrg(eq)))
        {
          tokCovered += num;
          typCovered++;
        }
        
        tokTotal += num;
      }
    }    
   
    LOG.info("[" + dict.getName() + (src ? "]: source" : "]: target") + " dictionary coverage " + df.format(100.0 * tokCovered / tokTotal) + "% tokens and " + df.format(100.0 * typCovered / (double)eqs.size()) + "% types.");
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
    else
    { LOG.error("Could not find corpus accessor for " + kind);
    }
    
    return accessor;
  }
  
  protected EuroParlCorpusAccessor getEuroParlAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.europarl.Path"); 
    String subDir = src ? Configurator.CONFIG.getString("corpora.europarl.SrcSubDir") : Configurator.CONFIG.getString("corpora.europarl.TrgSubDir");

    SimpleDateFormat sdf = new SimpleDateFormat( "yy-MM-dd" );
    Date fromDate = sdf.parse(Configurator.CONFIG.getString("corpora.europarl.DateFrom"));
    Date toDate = sdf.parse(Configurator.CONFIG.getString("corpora.europarl.DateTo"));
    
    return new EuroParlCorpusAccessor(appendSep(path) + subDir, fromDate, toDate);
  }
  
  protected CrawlCorpusAccessor getCrawlsAccessor(boolean src) throws Exception
  {    
    String path = Configurator.CONFIG.getString("corpora.crawls.Path"); 
    String subDir = src ? Configurator.CONFIG.getString("corpora.crawls.SrcSubDir") : Configurator.CONFIG.getString("corpora.crawls.TrgSubDir");

    SimpleDateFormat sdf = new SimpleDateFormat( "yy-MM-dd" );
    Date fromDate = sdf.parse(Configurator.CONFIG.getString("corpora.crawls.DateFrom"));
    Date toDate = sdf.parse(Configurator.CONFIG.getString("corpora.crawls.DateTo"));
    
    return new CrawlCorpusAccessor(appendSep(path) + subDir, fromDate, toDate);
  }
  
  protected LexCorpusAccessor getWikiAccessor(boolean src)
  {
    String path = Configurator.CONFIG.getString("corpora.wiki.Path");
    String fileRegExp = src ? Configurator.CONFIG.getString("corpora.wiki.SrcRegExp") : Configurator.CONFIG.getString("corpora.wiki.TrgRegExp");
  
    return new LexCorpusAccessor(fileRegExp, appendSep(path));
  }
  
  protected void prepareDictionariesFromSingleFile(
      Set<EquivalenceClass> srcContEqs, Set<EquivalenceClass> trgContEqs,
      Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs) throws Exception
  {
    String dictDir = Configurator.CONFIG.getString("resources.dictionary.Path");
    String dictFileName = Configurator.CONFIG.getString("resources.dictionary.Dictionary");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;

    LOG.info("Reading/preparing dictionaries ...");
    
    m_entireDict = new SimpleDictionary(dictDir + dictFileName, "Entire", filterRomanTrg);
    m_entireDict.pruneCounts(ridDictNumTrans);
    
    // Split into seed and test dictionaries
    DictPair pair;
    
    if (Configurator.CONFIG.containsKey("experiments.DictionaryPercentToUse") && Configurator.CONFIG.containsKey("experiments.DictionaryNumberToUse"))
    { throw new IllegalArgumentException("Both percentage and number defined");
    }
    else if (Configurator.CONFIG.containsKey("experiments.DictionaryPercentToUse"))
    {
      double percentSeedDict = Configurator.CONFIG.getDouble("experiments.DictionaryPercentToUse");
      pair = m_entireDict.splitPercent(percentSeedDict * 100 + "%-of-Usable", (1.0 - percentSeedDict) * 100 + "%-of-Usable", percentSeedDict);
    }
    else if (Configurator.CONFIG.containsKey("experiments.DictionaryNumberToUse"))
    {
      int numberSeedDict = Configurator.CONFIG.getInt("experiments.DictionaryNumberToUse");
      pair = m_entireDict.splitPart(numberSeedDict + "-entries-from-Usable", (m_entireDict.size() - numberSeedDict) + "-entries-from-Usable", numberSeedDict);
    }
    else
    { throw new IllegalArgumentException();
    }
       
    m_seedDict = new Dictionary(srcContEqs, trgContEqs, pair.dict, pair.dict.getName());
    m_testDict = new Dictionary(srcEqs, trgEqs, pair.rest, pair.rest.getName());
    
    LOG.info("Entire Dictionary: " + m_entireDict.toString());    
    LOG.info("Seed Dictionary: " + m_seedDict.toString());
    LOG.info("Test Dictionary: " + m_testDict.toString());
  }

  /*
  protected void prepareSingleLangDictionaries(boolean src) throws Exception
  {  
    String eqClassName = src ? Configurator.CONFIG.getString("preprocessing.SrcEqClass") : Configurator.CONFIG.getString("preprocessing.TrgEqClass");
    Set<EquivalenceClass> eqClasses = src ? m_allSrcEqs : m_allTrgEqs;
    
    LOG.info("Preparing monolingual dictionaries ...");
    
    m_entireDict = new Dictionary(eqClassName, eqClassName, "Monolingual-Dict");
        
    for (EquivalenceClass eq : eqClasses)
    { m_entireDict.add(eq, eq);
    }
    
    m_seedDict = m_testDict = m_entireDict;
    LOG.info("Use, seed and test dictionary: " + m_entireDict.toString());
  }
*/

  protected void assignTypeProp(Set<? extends EquivalenceClass> eqClasses, EqType type)
  {
    for (EquivalenceClass eq : eqClasses)
    { eq.setProperty(new Type(type));
    }
  }

  /** Selects most frequent test dictionary tokens for induction. */
  protected Set<EquivalenceClass> selectMostFrequentSrcTokens(Dictionary testDict, Set<EquivalenceClass> srcEqs) throws IOException
  {
    int numToKeep = Configurator.CONFIG.containsKey("experiments.NumSource") ? Configurator.CONFIG.getInt("experiments.NumSource") : -1;
    String outDir = Configurator.CONFIG.getString("output.Path");
    Set<EquivalenceClass> srcSubset = new HashSet<EquivalenceClass>(srcEqs);
    
    srcSubset.retainAll(testDict.getAllSrc());
    
    if ((numToKeep >= 0) && (srcSubset.size() > numToKeep))
    {
      LinkedList<EquivalenceClass> valList = new LinkedList<EquivalenceClass>(srcSubset);
      Collections.sort(valList, new NumberComparator(false));
      
      for (int i = numToKeep; i < valList.size(); i++)
      {
        srcSubset.remove(valList.get(i));
      }
    }
    
    BufferedWriter writer = new BufferedWriter(new FileWriter(outDir + SRC_TO_INDUCT));
    
    for (EquivalenceClass eq : srcSubset)
    { writer.write(((Number)eq.getProperty(Number.class.getName())).getNumber() + "\t" + eq.toString() + "\n");
    }
    
    writer.close();
    
    LOG.info("Selected " + srcSubset.size() + " most frequent test dictionary source classes (see " + outDir + SRC_TO_INDUCT + ").");

    return srcSubset;
  }

  /** Selects random test dictionary tokens for induction. */
  protected Set<EquivalenceClass> selectRandSrcTokens(Dictionary testDict, Set<EquivalenceClass> srcEqs) throws Exception
  {
    int numToKeep = Configurator.CONFIG.containsKey("experiments.NumSource") ? Configurator.CONFIG.getInt("experiments.NumSource") : -1;
    String outDir = Configurator.CONFIG.getString("output.Path");
    Set<EquivalenceClass> srcSubset = new HashSet<EquivalenceClass>(srcEqs);
    
    srcSubset.retainAll(testDict.getAllSrc());
    
    if ((numToKeep >= 0) && (srcSubset.size() > numToKeep))
    {
      LinkedList<EquivalenceClass> valList = new LinkedList<EquivalenceClass>(srcSubset);
      srcSubset.clear();

      for (int i = 0; i < numToKeep; i++)
      { srcSubset.add(valList.remove(m_rand.nextInt(valList.size())));
      }
    }
        
    BufferedWriter writer = new BufferedWriter(new FileWriter(outDir + SRC_TO_INDUCT));
    
    for (EquivalenceClass eq : srcSubset)
    { writer.write(((Number)eq.getProperty(Number.class.getName())).getNumber()  + "\t" + eq.toString() + "\n");
    }
    
    writer.close();
    
    LOG.info("Selected " + srcSubset.size() + " random test dictionary source classes (see " + outDir + SRC_TO_INDUCT + ").");

    return srcSubset;
  }
  
  protected String appendSep(String str)
  {
    String ret = (str == null) ? null : str.trim();
    
    if (ret != null && ret.length() > 0 && !ret.endsWith(File.separator))
    { ret += File.separator; 
    }
    
    return ret;
  }

  protected SimpleDictionary m_entireDict;
  protected Dictionary m_seedDict;
  protected Dictionary m_testDict;
  protected Set<EquivalenceClass> m_allSrcEqs;
  protected Set<EquivalenceClass> m_allTrgEqs;
  protected Set<EquivalenceClass> m_srcEqs;
  protected Set<EquivalenceClass> m_trgEqs;
  protected double m_numToksInSrc;
  protected double m_numToksInTrg;
  protected double m_maxTokCountInSrc;
  protected double m_maxTokCountInTrg;  
  protected Random m_rand = new Random(1);

}