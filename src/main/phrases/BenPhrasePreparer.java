package main.phrases;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

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
import babel.content.eqclasses.properties.context.PhraseContextCollector;
import babel.content.eqclasses.properties.lshcontext.LSHContext;
import babel.content.eqclasses.properties.lshcontext.LSHContextCollector;
import babel.content.eqclasses.properties.lshtime.LSHTimeDistribution;
import babel.content.eqclasses.properties.lshtime.LSHTimeDistributionCollector;
import babel.content.eqclasses.properties.number.Number;
import babel.content.eqclasses.properties.number.NumberCollector;
import babel.content.eqclasses.properties.number.PhraseNumberCollector;
import babel.content.eqclasses.properties.time.PhraseTimeDistributionCollector;
import babel.content.eqclasses.properties.type.Type;
import babel.content.eqclasses.properties.type.Type.EqType;
import babel.ranking.scorers.Scorer;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.SimpleDictionary;
import babel.util.dict.SimpleDictionary.DictHalves;

public class BenPhrasePreparer {
  protected static final Log LOG = LogFactory.getLog(BenPhrasePreparer.class);
  protected static final String DEFAULT_CHARSET = "UTF-8";
  protected static final String FIELD_DELIM = " ||| ";
  
  public Dictionary getSeedDict() {
    return m_seedDict;
  }
  
  public long getMaxSrcTokCount(){
    return m_maxTokCountInSrc;
  }
  
  public long getMaxTrgTokCount() {
    return m_maxTokCountInTrg; 
  }
  
  public long getMaxPhrCount() {
    return m_maxPhrCount; 
  }
  
  public void openFiles(String phrasesFile, String contextSigFile, String timeSigFile) throws Exception {
    m_phraseStrWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(phrasesFile, false), DEFAULT_CHARSET));    
    m_binContextStream = new BufferedOutputStream(new FileOutputStream(contextSigFile, false));    
    m_binTimeStream = new BufferedOutputStream(new FileOutputStream(timeSigFile, false));    
  }
  
  public void closeFiles() throws Exception {
    m_phraseStrWriter.close();
    m_binContextStream.close();
    m_binTimeStream.close();   
  }
  
  public void saveChunk(Set<Phrase> phrases) throws Exception {  
    List<Phrase> phraseList = new ArrayList<Phrase>(phrases);
    Collections.sort(phraseList, new LexComparator(true));
    LSHContext context;
    LSHTimeDistribution time;
      
    for (Phrase phrase : phraseList) {
      context = (LSHContext)(phrase.getProperty(LSHContext.class.getName()));
      time = (LSHTimeDistribution)(phrase.getProperty(LSHTimeDistribution.class.getName()));

      m_phraseStrWriter.write(phrase.getStem() + "\n");
      m_binContextStream.write(context.getSignature());
      m_binTimeStream.write(time.getSignature());

      m_phraseStrWriter.flush();
      m_binContextStream.flush();
      m_binTimeStream.flush();
    }
  }
  
 public void collectContextAndTimeProps(boolean src, Set<Phrase> chunk) throws Exception{
    
    LOG.info(" - Collecting context and time phrase properties for " + chunk.size() + (src ? " source " : " target ") + "phrases ...");
    
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    int contextWindowSize = Configurator.CONFIG.getInt("preprocessing.context.Window");

    collectContextAndTimeProps(src, chunk, maxPhraseLength, src ? m_contextSrcEqs : m_contextTrgEqs, contextWindowSize, true);
  }
  
  protected void collectContextAndTimeProps(boolean src, Set<Phrase> phrases, int maxPhraseLength, Set<EquivalenceClass> contextEqs, int contextWindowSize, boolean caseSensitive) throws Exception {

    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);    
    (new PhraseContextCollector(maxPhraseLength, caseSensitive, contextWindowSize, contextWindowSize, contextEqs)).collectProperty(accessor, phrases);

    accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Time"), src);
    PhraseTimeDistributionCollector distCollector = new PhraseTimeDistributionCollector(maxPhraseLength, caseSensitive);
    distCollector.collectProperty(accessor, phrases);      
  }

  public Set<Phrase> prepareNextChunk(boolean src, String fileName, int chunkSize) throws Exception {
    
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    
    Set<Phrase> chunk = readPhraseFileChunk(src, fileName, chunkSize, caseSensitive);

    collectNumberProps(src, chunk, maxPhraseLength, caseSensitive);
    assignTypeProp(chunk, src ? EqType.SOURCE : EqType.TARGET);

    return chunk;
  }
  
  public void prepareContextAndTimeProps(boolean src, Set<? extends EquivalenceClass> eqs, Scorer contextScorer, Scorer timeScorer) throws Exception {
    
    LOG.info(" - " + (src ? "Preparing source" : "Projecting and preparing target") + " contextual items with " + contextScorer.toString() + " and time distributions with " + timeScorer.toString() + "...");
    for (EquivalenceClass eq : eqs) { 
      contextScorer.prepare(eq);
      timeScorer.prepare(eq);
    }

    LOG.info(" - Mapping " + (src ? "source" : "target") + " context into LSH space...");    
    (new LSHContextCollector(true)).collectProperty(eqs);
    LOG.info(" - Mapping " + (src ? "source" : "target") + " temporal into LSH space...");    
    (new LSHTimeDistributionCollector(true)).collectProperty(eqs);
  }
  
  public void prepareForChunkCollection(boolean src, String fileName, int numLines) throws Exception {
    int maxPhraseLength = Configurator.CONFIG.getInt("preprocessing.phrases.MaxPhraseLength");
    boolean caseSensitive = Configurator.CONFIG.getBoolean("preprocessing.phrases.CaseSensitive");
    
    m_maxPhrCount = findMaxPhraseCount(src, fileName, numLines, maxPhraseLength, caseSensitive);
  }
  
  public void prepareContextForChunkCollection() throws Exception {
    collectContextEqs();
    prepareSeedDictionary(m_contextSrcEqs, m_contextTrgEqs);
    filterContextEqs();
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
  
  @SuppressWarnings("unchecked")
  protected void collectContextEqs() throws Exception {
    
    LOG.info(" - Constructing contextual equivalence classes...");
    
    boolean filterRomanSrc = Configurator.CONFIG.containsKey("preprocessing.FilterRomanSrc") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanSrc");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    Class<EquivalenceClass> srcContClassClass = (Class<EquivalenceClass>)Class.forName(Configurator.CONFIG.getString("preprocessing.context.SrcEqClass"));
    Class<EquivalenceClass> trgContClassClass = (Class<EquivalenceClass>)Class.forName(Configurator.CONFIG.getString("preprocessing.context.TrgEqClass"));
    
    m_contextSrcEqs = collectContextEqs(true, true, filterRomanSrc, srcContClassClass);
    m_contextTrgEqs = collectContextEqs(false, true, filterRomanTrg, trgContClassClass);
   
    m_maxTokCountInSrc = findMaxCount(m_contextSrcEqs);
    m_maxTokCountInTrg = findMaxCount(m_contextTrgEqs);
    
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
  
  protected void assignTypeProp(Set<? extends EquivalenceClass> eqClasses, EqType type) {
    
    Type commonType = new Type(type);
    
    for (EquivalenceClass eq : eqClasses)
    { eq.setProperty(commonType);
    }
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
  
  protected Set<Phrase> readPhraseFileChunk(boolean src, String fileName, int numLines, boolean caseSensitive) throws IOException {
    
    String line = null;
    Phrase phrase;
    int numLinesRead = 0;
    HashSet<Phrase> phraseChunk = new HashSet<Phrase>();
    
    // If we got a new file - set up a new reader for it      
    if (!fileName.equals(m_phraseFileName)) {
      m_phraseFileName = fileName;
        
      if (m_phraseFileReader != null) {
        m_phraseFileReader.close();
      }
       
      InputStream is = new FileInputStream(fileName);
        
      if (fileName.toLowerCase().endsWith("gz"))
      { is = new GZIPInputStream(is);
      }
        
      m_phraseFileReader = new BufferedReader(new InputStreamReader(is, DEFAULT_CHARSET));
    }
    
    while (((numLines < 0) || (numLinesRead < numLines)) &&
           ((line = m_phraseFileReader.readLine()) != null)) {
      
      numLinesRead++;
      
      (phrase = new Phrase()).init(line, caseSensitive);    
      phrase.assignId();
      phraseChunk.add(phrase);
    }
    
    if ((line == null) && (phraseChunk.size() == 0)) {
      m_phraseFileName = "";
      m_phraseFileReader = null;
    }
    
    return phraseChunk;
  }
  
  protected long findMaxPhraseCount(boolean src, String fileName, int chunkSize, int maxPhraseLength, boolean caseSensitive) throws Exception {
    
    LOG.info(" - Collecting max count for " + (src ? "source" : "target") + " phrases");
    
    // Go though phrases in chunks and compute max occur numbers
    long maxCount = 0;
    Set<Phrase> chunk;
    
    while ((chunk = readPhraseFileChunk(src, fileName, chunkSize, caseSensitive)).size() > 0) {
      collectNumberProps(src, chunk, maxPhraseLength, caseSensitive);
      maxCount = Math.max(maxCount, findMaxCount(chunk));
    }
    
    LOG.info(" - " + (src ? "Source" : "Target") + " phrases max count = " + maxCount);
    
    return maxCount;
  }
  
  protected long findMaxCount(Set<? extends EquivalenceClass> eqs) {

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
  
  protected void collectNumberProps(boolean src, Set<Phrase> phrases, int maxPhraseLength, boolean caseSensitive) throws Exception {
    // Collect counts
    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), src);
    (new PhraseNumberCollector(maxPhraseLength, caseSensitive)).collectProperty(accessor, phrases);
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
  
  protected String m_phraseFileName = "";
  protected BufferedReader m_phraseFileReader;
  protected long m_maxPhrCount = 0;
  protected Dictionary m_seedDict = null;
  protected Set<EquivalenceClass> m_contextSrcEqs = null;
  protected Set<EquivalenceClass> m_contextTrgEqs = null;
  protected long m_maxTokCountInSrc = 0;
  protected long m_maxTokCountInTrg = 0;
  
  protected BufferedWriter m_phraseStrWriter;    
  protected BufferedOutputStream m_binContextStream;    
  protected BufferedOutputStream m_binTimeStream;
}
