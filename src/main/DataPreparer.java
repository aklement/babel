package main;

import java.io.File;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedList;
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
import babel.content.eqclasses.filters.LengthFilter;
import babel.content.eqclasses.filters.NoContextFilter;
import babel.content.eqclasses.filters.NoTimeDistributionFilter;
import babel.content.eqclasses.filters.NumOccurencesFilter;
import babel.content.eqclasses.filters.RomanizationFilter;
import babel.content.eqclasses.filters.StopWordsFilter;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.ContextCollector;
import babel.content.eqclasses.properties.Number;
import babel.content.eqclasses.properties.NumberAndTypeCollector;
import babel.content.eqclasses.properties.TimeDistribution;
import babel.content.eqclasses.properties.TimeDistributionCollector;
import babel.content.eqclasses.properties.Type;
import babel.content.eqclasses.properties.Type.EqType;

import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.Dictionary.DictPair;
import babel.util.persistence.EqClassPersister;

public class DataPreparer
{
  protected static final Log LOG = LogFactory.getLog(DataPreparer.class);
  
  protected static final String SRC_MAP_FILE = "src.map";
  protected static final String TRG_MAP_FILE = "trg.map";

  public void prepare() throws Exception
  { 
    LOG.info("Reading/preparing seed and context dictionaries ...");
    //prepareDictionaries();  <--
    prepareDictionariesFromSingleFile();
    
    // Read / collect eq classes and their properties
    try
    {
      LOG.info("Reading previously collected eq. classes / properties ...");
      readEqClasses();
      readProps();
    }
    catch (Exception e)
    {
      LOG.info("Failed to read eq. classes / properties (" + e.toString() + "), collecting from scratch ...");
      CorpusAccessor srcAccessor = getEuroParlAccessor(true);
      CorpusAccessor trgAccessor = getEuroParlAccessor(false);
      
      collectAndWriteEqClasses(srcAccessor, trgAccessor);
      collectAndWriteProps(m_seedDict, srcAccessor, trgAccessor);
    }
    
    // Measure dictionary coverage
    dictCoverage(m_seedDict, m_allSrcEqs, true);
    dictCoverage(m_seedDict, m_allTrgEqs, false);
    collectTokenCounts(m_allSrcEqs, m_allTrgEqs);
    
    // Prune source and target candidates  
    pruneEqClasses();
  }
  
  public Dictionary getSeedDict()
  { return m_seedDict;
  }
  
  public Dictionary getUseDict()
  { return m_useDict;
  }
  
  public Dictionary getTestDict() 
  { return m_testDict;
  }
  
  public Set<EquivalenceClass> getSrcEqs()
  { return m_allSrcEqs;
  }
  
  public Set<EquivalenceClass> getTrgEqs()
  { return m_allTrgEqs;
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
  
  @SuppressWarnings("unchecked")
  protected void readEqClasses() throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");     
    String srcEqClassName = Configurator.CONFIG.getString("preprocessing.SrcEqClass");
    String trgEqClassName = Configurator.CONFIG.getString("preprocessing.TrgEqClass");
    
    LOG.info("Reading equivalence classes from " + preProcDir);
    
    // Extract the equivalence classes themselves
    m_allSrcEqs = EqClassPersister.unpersistEqClasses((Class<? extends EquivalenceClass>)Class.forName(srcEqClassName), preProcDir + SRC_MAP_FILE);
    m_allTrgEqs = EqClassPersister.unpersistEqClasses((Class<? extends EquivalenceClass>)Class.forName(trgEqClassName), preProcDir + TRG_MAP_FILE);

    LOG.info("All source classes: " + m_allSrcEqs.size());
    LOG.info("All target classes: " + m_allTrgEqs.size());
    
    LOG.info("Reading corpus counts ...");
    
    // Read all of their properties
    EqClassPersister.unpersistProperty(m_allSrcEqs, Number.class, preProcDir + Number.class.getSimpleName() + ".src.map"); 
    EqClassPersister.unpersistProperty(m_allTrgEqs, Number.class, preProcDir + Number.class.getSimpleName() + ".trg.map");
    EqClassPersister.unpersistProperty(m_allSrcEqs, Type.class, preProcDir + Type.class.getSimpleName() + ".src.map");
    EqClassPersister.unpersistProperty(m_allTrgEqs, Type.class, preProcDir + Type.class.getSimpleName() + ".trg.map");
  }
  
  protected void collectAndWriteEqClasses(CorpusAccessor srcAccessor, CorpusAccessor trgAccessor) throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");  
    String srcEqClasses = Configurator.CONFIG.getString("preprocessing.SrcEqClass");
    String trgEqClasses = Configurator.CONFIG.getString("preprocessing.TrgEqClass");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");

    // Get all equivalence classes for source and target (subject to a couple of simple filters)
    LOG.info("Collecting equivalence classes ...");

    // Collect for source
    ArrayList<EquivalenceClassFilter> filters = new ArrayList<EquivalenceClassFilter>(3);
    filters.add(new GarbageFilter());
    filters.add(new LengthFilter(2));
    
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(srcEqClasses, filters, false);
    m_allSrcEqs = collector.collect(srcAccessor.getCorpusReader(), -1);
    
    // Collect for target
    if (filterRomanTrg)
    { filters.add(new RomanizationFilter());
    }
    collector = new SimpleEquivalenceClassCollector(trgEqClasses, filters, false);
    m_allTrgEqs = collector.collect(trgAccessor.getCorpusReader(), -1);    
    
    EqClassPersister.persistEqClasses(m_allSrcEqs, preProcDir + SRC_MAP_FILE);
    EqClassPersister.persistEqClasses(m_allTrgEqs, preProcDir + TRG_MAP_FILE);
    
    LOG.info("All source classes: " + m_allSrcEqs.size());
    LOG.info("All target classes: " + m_allTrgEqs.size() + (filterRomanTrg ? " (without romanization) " : ""));
    
    LOG.info("Collecting corpus counts...");
    (new NumberAndTypeCollector(SimpleEquivalenceClass.class.getName(), false, EqType.SOURCE)).collectProperty(srcAccessor, m_allSrcEqs);    
    (new NumberAndTypeCollector(SimpleEquivalenceClass.class.getName(), false, EqType.TARGET)).collectProperty(trgAccessor, m_allTrgEqs);
    
    EqClassPersister.persistProperty(m_allSrcEqs, Number.class, preProcDir + Number.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_allTrgEqs, Number.class, preProcDir + Number.class.getSimpleName() + ".trg.map");
    EqClassPersister.persistProperty(m_allSrcEqs, Type.class, preProcDir + Type.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_allTrgEqs, Type.class, preProcDir + Type.class.getSimpleName() + ".trg.map");
  }
   
  protected void readProps() throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");     

    LOG.info("Reading context...");
    EqClassPersister.unpersistProperty(m_allSrcEqs, Context.class, preProcDir + Context.class.getSimpleName() + ".src.map");
    EqClassPersister.unpersistProperty(m_allTrgEqs, Context.class, preProcDir + Context.class.getSimpleName() + ".trg.map");
    
    LOG.info("Reading temporal distributions...");
    EqClassPersister.unpersistProperty(m_allSrcEqs, TimeDistribution.class, preProcDir + TimeDistribution.class.getSimpleName() + ".src.map");
    EqClassPersister.unpersistProperty(m_allTrgEqs, TimeDistribution.class, preProcDir + TimeDistribution.class.getSimpleName() + ".trg.map"); 
  }
  
  protected void collectAndWriteProps(Dictionary contextDict, CorpusAccessor srcAccessor, CorpusAccessor trgAccessor) throws Exception
  {
    String preProcDir = Configurator.CONFIG.getString("preprocessing.Path");  
    int pruneContEqIfOccursFewerThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursFewerThan");
    int pruneContEqIfOccursMoreThan = Configurator.CONFIG.getInt("preprocessing.context.PruneEqIfOccursMoreThan");
    
    LOG.info("Preparing contextual words: keeping those in dict [" + contextDict.toString() + "] and occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times...");    
    Set<EquivalenceClass> srcContextEq = new HashSet<EquivalenceClass>(m_allSrcEqs);
    Set<EquivalenceClass> trgContextEq = new HashSet<EquivalenceClass>(m_allTrgEqs);
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    
    filters.add(new DictionaryFilter(contextDict, true, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    srcContextEq = EquivalenceClassCollector.filter(srcContextEq, filters);

    filters.clear();
    filters.add(new DictionaryFilter(contextDict, true, false)); 
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    trgContextEq = EquivalenceClassCollector.filter(trgContextEq, filters);
    
    LOG.info("Context source classes: " + srcContextEq.size());
    LOG.info("Context target classes: " + trgContextEq.size());
    
    LOG.info("Collecting context...");
    (new ContextCollector(SimpleEquivalenceClass.class.getName(), false, 2, 2, srcContextEq)).collectProperty(srcAccessor, m_allSrcEqs);    
    (new ContextCollector(SimpleEquivalenceClass.class.getName(), false, 2, 2, trgContextEq)).collectProperty(trgAccessor, m_allTrgEqs);
        
    LOG.info("Collecting temporal distributions...");
    (new TimeDistributionCollector(SimpleEquivalenceClass.class.getName(), false, 1, false)).collectProperty(srcAccessor, m_allSrcEqs);    
    (new TimeDistributionCollector(SimpleEquivalenceClass.class.getName(), false, 1, false)).collectProperty(trgAccessor, m_allTrgEqs);
    
    // Save collected propereties
    EqClassPersister.persistProperty(m_allSrcEqs, Context.class, preProcDir + Context.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_allTrgEqs, Context.class, preProcDir + Context.class.getSimpleName() + ".trg.map");
    EqClassPersister.persistProperty(m_allSrcEqs, TimeDistribution.class, preProcDir + TimeDistribution.class.getSimpleName() + ".src.map");
    EqClassPersister.persistProperty(m_allTrgEqs, TimeDistribution.class, preProcDir + TimeDistribution.class.getSimpleName() + ".trg.map");  
  }
  
  protected void pruneEqClasses() throws Exception
  {
    String stopWordsDir = Configurator.CONFIG.getString("resources.stopwords.Path");
    String srcStop = Configurator.CONFIG.getString("resources.stopwords.SrcStopWords");
    String trgStop = Configurator.CONFIG.getString("resources.stopwords.TrgStopWords");
    String srcEqClasses = Configurator.CONFIG.getString("preprocessing.SrcEqClass");
    String trgEqClasses = Configurator.CONFIG.getString("preprocessing.TrgEqClass");
    int pruneCandIfOccursFewerThan = Configurator.CONFIG.getInt("preprocessing.candidates.PruneIfOccursFewerThan");
    int pruneCandIfOccursMoreThan = Configurator.CONFIG.getInt("preprocessing.candidates.PruneIfOccursMoreThan");    
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");

    LOG.info("Reading stop words ...");

    // Collect for source
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new GarbageFilter());
    
    SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(srcEqClasses, filters, false);
    Set<EquivalenceClass> srcStopEq = ((new File(stopWordsDir + srcStop)).exists()) ?
      collector.collect((new LexCorpusAccessor(srcStop, stopWordsDir)).getCorpusReader(), -1) :
      new HashSet<EquivalenceClass>();
    
    // Collect for target
    if (filterRomanTrg)
    { filters.add(new RomanizationFilter());
    }
    
    collector = new SimpleEquivalenceClassCollector(trgEqClasses, filters, false);
    Set<EquivalenceClass> trgStopEq = ((new File(stopWordsDir + trgStop)).exists()) ? 
      collector.collect((new LexCorpusAccessor(trgStop, stopWordsDir)).getCorpusReader(),-1) :
      new HashSet<EquivalenceClass>();
    
    LOG.info("Source stop words:" + srcStopEq.size() + ", target stop words:" + trgStopEq.size() + (filterRomanTrg ? " (without romanization) " : ""));  
    LOG.info("Pruning candidate words: removing stop words, eq classes with no context or distro, and keeping if occur (" + pruneCandIfOccursFewerThan + "," + pruneCandIfOccursMoreThan + ") times..."); 
    
    filters.clear();
    filters.add(new StopWordsFilter(srcStopEq));
    filters.add(new NoContextFilter());
    filters.add(new NoTimeDistributionFilter());
    filters.add(new NumOccurencesFilter(pruneCandIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursMoreThan, false));
    m_allSrcEqs = EquivalenceClassCollector.filter(m_allSrcEqs, filters);
    
    filters.clear();
    filters.add(new StopWordsFilter(trgStopEq));
    filters.add(new NoContextFilter());
    filters.add(new NoTimeDistributionFilter());
    filters.add(new NumOccurencesFilter(pruneCandIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneCandIfOccursMoreThan, false));
    m_allTrgEqs = EquivalenceClassCollector.filter(m_allTrgEqs, filters);
    
    LOG.info("Candidate source classes: " + m_allSrcEqs.size());
    LOG.info("Candidate target classes: " + m_allTrgEqs.size());
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
  
  protected void prepareDictionariesFromSingleFile() throws Exception
  {
    String dictDir = Configurator.CONFIG.getString("resources.dictionaries.Path");
    String useDictFile = Configurator.CONFIG.getString("resources.dictionaries.UseDictionary");
    String augDictFile = Configurator.CONFIG.containsKey("experiments.AugDictionary") ? Configurator.CONFIG.getString("experiments.AugDictionary") : null;
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
    
    m_useDict = new Dictionary(dictDir + useDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Usable", filterRomanTrg);
    m_useDict.pruneCounts(ridDictNumTrans);
    
    // Create seed and test dictionaries
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
    
    LOG.info("Use Dictionary: " + m_useDict.toString());    
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
    String dictDir = Configurator.CONFIG.getString("resources.DictPath");
    String testDictFile = Configurator.CONFIG.getString("resources.dictionaries.TestDictionary");
    String useDictFile = Configurator.CONFIG.getString("resources.dictionaries.UseDictionary");
    String augDictFile = Configurator.CONFIG.containsKey("experiments.AugDictionary") ? Configurator.CONFIG.getString("experiments.AugDictionary") : null;
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;

    m_useDict = new Dictionary(dictDir + useDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Usable", filterRomanTrg);
    m_useDict.pruneCounts(ridDictNumTrans);
    
    m_testDict = new Dictionary(dictDir + testDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Test", filterRomanTrg);
    m_testDict.pruneCounts(ridDictNumTrans);
    
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
    
    LOG.info("Use Dictionary: " + m_useDict.toString());
    LOG.info("Test Dictionary: " + m_testDict.toString());
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
  
  protected Dictionary m_seedDict;
  protected Dictionary m_useDict;
  protected Dictionary m_testDict;
  protected Set<EquivalenceClass> m_allSrcEqs;
  protected Set<EquivalenceClass> m_allTrgEqs;
  protected double m_numToksInSrc;
  protected double m_numToksInTrg;  
  protected double m_maxTokCountInSrc;
  protected double m_maxTokCountInTrg;  
}
