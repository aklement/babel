package main.lexinduct;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
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
import babel.content.eqclasses.comparators.NumberComparator;
import babel.content.eqclasses.filters.DictionaryFilter;
import babel.content.eqclasses.filters.EquivalenceClassFilter;
import babel.content.eqclasses.filters.GarbageFilter;
import babel.content.eqclasses.filters.LengthFilter;
import babel.content.eqclasses.filters.NumOccurencesFilter;
import babel.content.eqclasses.filters.RomanizationFilter;
import babel.content.eqclasses.filters.StopWordsFilter;
import babel.content.eqclasses.properties.context.PhraseContextCollector;
import babel.content.eqclasses.properties.lshcontext.LSHContextCollector;
import babel.content.eqclasses.properties.lshtime.LSHTimeDistributionCollector;
import babel.content.eqclasses.properties.number.Number;
import babel.content.eqclasses.properties.number.NumberCollector;
import babel.content.eqclasses.properties.time.PhraseTimeDistributionCollector;
import babel.content.eqclasses.properties.time.TimeDistribution;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.ranking.scorers.Scorer;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.SimpleDictionary;
import babel.util.dict.SimpleDictionary.DictHalves;

public class FreqBinInductPreparer {

  protected static final Log LOG = LogFactory.getLog(FreqBinInductPreparer.class);

  protected static final String DEFAULT_CHARSET = "UTF-8";

  protected static final int SRC_LOW_THRESH = 10;
  protected static final int SRC_HI_THRESH = 5000;
  protected static final int TRG_LOW_THRESH = 10;
  protected static final int TRG_HI_THRESH = Integer.MAX_VALUE;
  
  public void prepare() throws Exception {
  
	//Collect equivalence classes based on monolingual corpus
    collectContextEqs();
    //Prepare dictionary for EVALUATION
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    //Prepare dictionary for PROJECTION
    prepareProjDictionary(m_contextSrcEqs, m_contextTrgEqs);
    //Loads transliteration dictionary, if there is one
    prepareTranslitDictionary(m_contextSrcEqs);
    selectSrcCandidatesByNum();
    //selectSrcCandidatesByFreq();
    selectTrgCandidates();
    filterContextEqs();
    LOG.info("Done with initial prep. Num src context eqs: "+m_contextSrcEqs.size()+" Num trg context eqs: "+m_contextTrgEqs.size());
  }
  
  public Set<EquivalenceClass> getSrcEqsToInduct() {
    return m_srcEqs;
  }
  
  public List<Set<EquivalenceClass>> getBinnedSrcEqs() {
    return m_binnedSrcEqs;
  }
  
  public Set<EquivalenceClass> getTrgEqs() {
    return m_trgEqs;
  }
 
  public Set<EquivalenceClass> getSrcContextEqs() {
	  return m_contextSrcEqs;
  }
  
  public Set<EquivalenceClass> getTrgContextEqs(){
	  return m_contextTrgEqs;
  }
   
  public Dictionary getSeedDict() {
    return m_seedDict;
  }

  public Dictionary getProjDict() {
	    return m_projDict;
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
  
 public void collectContextAndTimeProps(Set<? extends EquivalenceClass> srcPhrases, Set<? extends EquivalenceClass> trgPhrases) throws Exception{
    
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
 
 protected void alignDistributions(Set<Integer> srcBins, Set<Integer> trgBins, Set<? extends EquivalenceClass> srcEqs, Set<? extends EquivalenceClass> trgEqs)
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
 
 public void prepareContextAndTimeProps(boolean src, Set<? extends EquivalenceClass> eqs, Scorer contextScorer, Scorer timeScorer, boolean mapToLSH) throws Exception {
   
   LOG.info(" - " + (src ? "Projecting and scoring source" : "Scoring target") + " contextual items with " + contextScorer.toString() + " and time distributions with " + timeScorer.toString() + "...");
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
  
  /**
   * @return time bins for which counts were collected
   */
  protected Set<Integer> collectContextAndTimeProps(boolean src, Set<? extends EquivalenceClass> phrases, int maxPhraseLength, Set<EquivalenceClass> contextEqs, int contextWindowSize, boolean caseSensitive) throws Exception {

    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);    
    (new PhraseContextCollector(maxPhraseLength, caseSensitive, contextWindowSize, contextWindowSize, contextEqs)).collectProperty(accessor, phrases);

    accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Time"), src);
    PhraseTimeDistributionCollector distCollector = new PhraseTimeDistributionCollector(maxPhraseLength, caseSensitive);
    distCollector.collectProperty(accessor, phrases);
      
    return distCollector.binsCollected();
  }
  
  protected void selectSrcCandidatesByNum() throws Exception {

    LinkedList<EquivalenceClass> filteredSrcEqs = new LinkedList<EquivalenceClass>(createAndFilterSrcEqs(m_contextSrcEqs, SRC_LOW_THRESH, SRC_HI_THRESH));
    Collections.sort(filteredSrcEqs, new NumberComparator(false));

    int numSource = Configurator.CONFIG.getInt("experiments.NumSource");
    int numBins = Configurator.CONFIG.getInt("experiments.NumSourceBins");
    int numInBin = filteredSrcEqs.size() / numBins;
    int numToSampleFromBin = Math.min(numSource, filteredSrcEqs.size()) / numBins;
 
    m_srcEqs = new HashSet<EquivalenceClass>();
    m_binnedSrcEqs = new LinkedList<Set<EquivalenceClass>>();
    
    Random rand = new Random();
    List<EquivalenceClass> bin;
    HashSet<EquivalenceClass> binSample;
    EquivalenceClass eq;
    double averageCount, maxCount, minCount, count;
    
    LOG.info(" - Selecting " + numSource + " source candidates from " + numBins + " bins ...");
    
    for (int binNum = 0; binNum < numBins; binNum++) {

      bin = new LinkedList<EquivalenceClass>(filteredSrcEqs.subList(0, numInBin));
      filteredSrcEqs.removeAll(bin);
      
      m_binnedSrcEqs.add(binSample = new HashSet<EquivalenceClass>());
      averageCount = 0;
      minCount = Double.MAX_VALUE;
      maxCount = Double.MIN_VALUE;
      
      for (int i = 0; i < numToSampleFromBin; i++) {
        
        eq = bin.remove(rand.nextInt(bin.size()));
        binSample.add(eq);
        m_srcEqs.add(eq);
        
        count = ((Number)eq.getProperty(Number.class.getName())).getNumber();
        if (count < minCount) {
          minCount = count;
        }
        
        if (count > maxCount) {
          maxCount = count;
        }
        
        averageCount += count;
      }
      
      LOG.info(" - Bin " + binNum + ": counts between " + minCount + " and " + maxCount + ", average = " + averageCount/(double)binSample.size() + " and " + binSample.size() + " source candidates ...");
    }
    
    LOG.info(" - Selected " + m_srcEqs.size() + " source candidates ...");
  }

  protected void selectSrcCandidatesByFreq() throws Exception {
    
    int numSource = Configurator.CONFIG.getInt("experiments.NumSource");
    int numBins = Configurator.CONFIG.getInt("experiments.NumSourceBins");
    int numInBin = numSource / numBins;
    double binSize = (Math.min(SRC_HI_THRESH, m_maxTokCountInSrc) - SRC_LOW_THRESH) / (double) numBins;
    assert binSize > 0;
  
    LOG.info(" - Selecting " + numSource + " source candidates from " + numBins + " frequency bins (size "+ (int) binSize +") ...");
    
    m_srcEqs = new HashSet<EquivalenceClass>();
    m_binnedSrcEqs = new LinkedList<Set<EquivalenceClass>>();

    LinkedList<EquivalenceClass> filteredSrcEqs = new LinkedList<EquivalenceClass>(createAndFilterSrcEqs(m_contextSrcEqs, SRC_LOW_THRESH, SRC_HI_THRESH));
    Collections.sort(filteredSrcEqs, new NumberComparator(false));
    LinkedList<EquivalenceClass> band = new LinkedList<EquivalenceClass>();
    
    double from;
    double to = Math.min(SRC_HI_THRESH, m_maxTokCountInSrc);
    EquivalenceClass eq;
    long eqNum;
    int j = 0;
    int numCollected = 0;
    Random rand = new Random();
    HashSet<EquivalenceClass> bandSet;
    
    while (to > SRC_LOW_THRESH) {
      
      from = to;
      to -= binSize;
      to = Math.max(to, SRC_LOW_THRESH);
      band.clear();
      
      for (; j < filteredSrcEqs.size(); j++) {
        eq = filteredSrcEqs.get(j);
        eqNum = ((Number)filteredSrcEqs.get(j).getProperty(Number.class.getName())).getNumber();
        
        if (eqNum <= from && eqNum > to) {
          band.add(eq);
        } else if (eqNum <= to) {
          break;
        }
      }
      
      numCollected = 0;
      m_binnedSrcEqs.add(bandSet = new HashSet<EquivalenceClass>());
      
      while (band.size() > 0 && numCollected < numInBin) {
        m_srcEqs.add(eq = band.remove(rand.nextInt(band.size())));
        bandSet.add(eq);
        numCollected++;
      }
      
      LOG.info(" - Bin " + (m_binnedSrcEqs.size() - 1) + ": [" + to + "," + from + "] has " + numCollected + " source candidates ...");
    }

    LOG.info(" - Selected " + m_srcEqs.size() + " source candidates ...");
  }
  
  public void writeSelectedCandidates(String fileName) throws Exception {
    
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileName), DEFAULT_CHARSET));    

    int num = 0;
    
    for (Set<EquivalenceClass> bin : m_binnedSrcEqs) {
      
      writer.write("-------------- Bin " + num++ + " --------------\n");

      for (EquivalenceClass eq : bin) {
        writer.write(((Number)eq.getProperty(Number.class.getName())).getNumber() + "\t" + eq.getStem() + "\n");
      }
    }
    
    writer.close();
  }
 
  protected void selectTrgCandidates() throws Exception {
    
    LOG.info(" - Selecting target candidates ...");
    m_trgEqs = createAndFilterTrgEqs(m_contextTrgEqs, TRG_LOW_THRESH, TRG_HI_THRESH);
    LOG.info(" - Selected " + m_trgEqs.size() + " target candidates ...");
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
  
  protected Set<EquivalenceClass> createAndFilterSrcEqs(Set<EquivalenceClass> contSrcEqs, int pruneContEqIfOccursFewerThan, int pruneContEqIfOccursMoreThan) throws Exception {

    String stopWordsDir = Configurator.CONFIG.getString("resources.stopwords.Path");
    String srcStopFileName = Configurator.CONFIG.containsKey("resources.stopwords.SrcStopWords") ? Configurator.CONFIG.getString("resources.stopwords.SrcStopWords") : null;
    
    LOG.info(" - Filtering source words: keeping those in dict [" + m_seedDict.toString() + "] and occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times" + ((srcStopFileName == null) ? " ..." : " and not in the stop word list ..."));
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new DictionaryFilter(m_seedDict, true, true)); 
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));

    if (srcStopFileName != null)
    {
      SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, false);
      Set<? extends EquivalenceClass> stopEqs = ((new File(stopWordsDir + srcStopFileName)).exists()) ?
          collector.collect((new LexCorpusAccessor(srcStopFileName, stopWordsDir, true)).getCorpusReader(), -1) :
          new HashSet<EquivalenceClass>();
          
      filters.add(new StopWordsFilter(stopEqs));    
    }
    
    Set<EquivalenceClass> filtSrcEqs = EquivalenceClassCollector.filter(contSrcEqs, filters);
    LOG.info(" - Filtered source classes: " + filtSrcEqs.size());
    
    return filtSrcEqs;
  }
  
  protected Set<EquivalenceClass> createAndFilterTrgEqs(Set<EquivalenceClass> contTrgEqs, int pruneContEqIfOccursFewerThan, int pruneContEqIfOccursMoreThan) throws Exception {

    String stopWordsDir = Configurator.CONFIG.getString("resources.stopwords.Path");
    String trgStopFileName = Configurator.CONFIG.containsKey("resources.stopwords.TrgStopWords") ? Configurator.CONFIG.getString("resources.stopwords.TrgStopWords") : null;
    
    LOG.info(" - Filtering target words: keeping those occuring (" + pruneContEqIfOccursFewerThan + "," + pruneContEqIfOccursMoreThan + ") times" + ((trgStopFileName == null) ? " ..." : " and not in the stop word list ..."));
    
    LinkedList<EquivalenceClassFilter> filters = new LinkedList<EquivalenceClassFilter>();
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursFewerThan, true));
    filters.add(new NumOccurencesFilter(pruneContEqIfOccursMoreThan, false));
    
    if (trgStopFileName != null)
    {
      SimpleEquivalenceClassCollector collector = new SimpleEquivalenceClassCollector(filters, false);
      Set<? extends EquivalenceClass> stopEqs = ((new File(stopWordsDir + trgStopFileName)).exists()) ?
          collector.collect((new LexCorpusAccessor(trgStopFileName, stopWordsDir, true)).getCorpusReader(), -1) :
          new HashSet<EquivalenceClass>();
          
      filters.add(new StopWordsFilter(stopEqs));    
    }

    Set<EquivalenceClass> filtTrgEqs = EquivalenceClassCollector.filter(contTrgEqs, filters);
    LOG.info(" - Filtered target classes: " + filtTrgEqs.size());
    
    return filtTrgEqs;
  }
  
  /***
   * Prepoares dictionary for evaluation (projection dictionary separate)
   * @param srcContEqs
   * @param trgContEqs
   * @throws Exception
   */
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

  /***
   * Given set of src Cont eqs and trg Cont eqs that appear in monolingual data, save a dictionary that contains translations between the two
   * @param srcContEqs
   * @param trgContEqs
   * @throws Exception
   */
  protected void prepareProjDictionary(Set<EquivalenceClass> srcContEqs, Set<EquivalenceClass> trgContEqs) throws Exception {
	    
	    String dictDir = Configurator.CONFIG.getString("resources.projdictionary.Path");
	    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.ProjDictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
	    SimpleDictionary simpProjDict;
	    
	    LOG.info(" - Reading/preparing seed dictionary ...");
	    
	    if (Configurator.CONFIG.containsKey("resources.projdictionary.Dictionary")) {
	      String dictFileName = Configurator.CONFIG.getString("resources.projdictionary.Dictionary");
	      simpProjDict = new SimpleDictionary(dictDir + dictFileName, "ProjectionDictionary");
	    } else {
	      String srcDictFileName = Configurator.CONFIG.getString("resources.projdictionary.SrcName");
	      String trgDictFileName = Configurator.CONFIG.getString("resources.projdictionary.TrgName");      
	      simpProjDict = new SimpleDictionary(new DictHalves(dictDir + srcDictFileName, dictDir + trgDictFileName) , "ProjectionDictionary");
	    }

	    simpProjDict.pruneCounts(ridDictNumTrans);
	    
	    m_projDict = new Dictionary(srcContEqs, trgContEqs, simpProjDict, "ProjDictionary");
	    
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
  
  protected Dictionary m_seedDict = null;
  protected Dictionary m_projDict = null;
  protected SimpleDictionary m_translitDict = null;

  protected List<Set<EquivalenceClass>> m_binnedSrcEqs = null;
  protected Set<EquivalenceClass> m_srcEqs = null;
  protected Set<EquivalenceClass> m_trgEqs = null;
  
  protected Set<EquivalenceClass> m_contextSrcEqs = null;
  protected Set<EquivalenceClass> m_contextTrgEqs = null;
    
  protected long m_maxTokCountInSrc = 0;
  protected long m_maxTokCountInTrg = 0;
}
