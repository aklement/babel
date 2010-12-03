package babel.content.eqclasses.phrases;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import babel.content.eqclasses.comparators.LexComparator;
import babel.util.misc.GettableHashSet;

public class PhraseTable {

  protected static final String DEFAULT_CHARSET = "UTF-8";
  public static final String FIELD_DELIM = " ||| ";

  public enum PairFeat {
    
    FE(0), 
    LEX_FE(1), 
    EF(2), 
    LEX_EF(3), 
    PHPENALTY(4),
    CONTEXT(5),
    TIME(6),
    EDIT(7);
    
    private PairFeat(final int idx) {
      this.idx = idx;
    }
    
    public final int idx;
  };
  
  public PhraseTable(String encoding, boolean caseSensitive) {
    m_encoding = encoding;
    m_phraseMap = new HashMap<Phrase, Map<Phrase, PairProps>>();
    m_caseSensitive = caseSensitive;
    m_numPairs = 0;
    m_curFileName = null;
    m_curFileReader = null;    
  }

  public PhraseTable(boolean caseSensitive) {
    this(DEFAULT_CHARSET, caseSensitive);
  }
  
  public PhraseTable(String phraseTableFile, int numLines, String encoding, boolean caseSensitive) throws IOException {
    this(encoding, caseSensitive);    
    processPhraseTableFile(phraseTableFile, numLines);
    closePhraseTableFile();
  }

  public PhraseTable(String phraseTableFile, int numLines, boolean caseSensitive) throws IOException {
    this(phraseTableFile, numLines, DEFAULT_CHARSET, caseSensitive);
  }
  
  public Set<Phrase> getAllSrcPhrases() {
    return m_phraseMap.keySet();
  }

  public int numSrcPhrases() {
    return m_phraseMap.size();
  }
  
  public int numPhrasePairs() {
    return m_numPairs;
  }
 
  public Set<Phrase> getAllTrgPhrases() {
    HashSet<Phrase> allTrg = new HashSet<Phrase>();
    
    for (Map<Phrase, PairProps> trgMap : m_phraseMap.values()) {
      allTrg.addAll(trgMap.keySet());
    }
    
    return allTrg;
  }
  
  public boolean removePairsWithSrc(Phrase srcPhrase) {
    return m_phraseMap.remove(srcPhrase) != null;
  }

  public void removePairsWithSrc(Set<Phrase> srcPhrases) {    
    for (Phrase srcPhrase : srcPhrases) {
      m_phraseMap.remove(srcPhrase);
    }     
  }
  
  public Set<Phrase> getTrgPhrases(Phrase srcPhrase) {
    return m_phraseMap.get(srcPhrase).keySet();
  }
  
  public PairProps getProps(Phrase srcPhrase, Phrase trgPhrase) {
    
    Map<Phrase, PairProps> trgMap = m_phraseMap.get(srcPhrase);    
    return trgMap == null ? null : trgMap.get(trgPhrase);
  }
  
  public void savePhraseTable(String monoPhraseTableFile, String addMonoPhraseTableFile) throws IOException {
    
    BufferedWriter monoWriter = (monoPhraseTableFile == null) ? null : new BufferedWriter(new OutputStreamWriter(new FileOutputStream(monoPhraseTableFile, true), m_encoding));    
    BufferedWriter addMonoWriter = (addMonoPhraseTableFile == null) ? null : new BufferedWriter(new OutputStreamWriter(new FileOutputStream(addMonoPhraseTableFile, true), m_encoding));    

    List<Phrase> srcPhraseList = new ArrayList<Phrase>(m_phraseMap.keySet());
    Collections.sort(srcPhraseList, new LexComparator(true));
    
    Map<Phrase, PairProps> trgMap;
    List<Phrase> trgPhraseList;
    PairFeat[] featsToWrite = new PairFeat[]{PairFeat.PHPENALTY, PairFeat.CONTEXT, PairFeat.TIME, PairFeat.EDIT};
    
    for (Phrase srcPhrase : srcPhraseList) {
      
      trgMap = m_phraseMap.get(srcPhrase);    
      trgPhraseList = new ArrayList<Phrase>(trgMap.keySet());
      Collections.sort(trgPhraseList, new LexComparator(true));
      
      for (Phrase trgPhrase : trgPhraseList) {
        if (monoWriter != null) {
          monoWriter.write(srcPhrase.getStem() + FIELD_DELIM + trgPhrase.getStem() + FIELD_DELIM + trgMap.get(trgPhrase).getPairFeatStr(featsToWrite) + "\n");
        }
        
        if (addMonoWriter != null) {
          addMonoWriter.write(srcPhrase.getStem() + FIELD_DELIM + trgPhrase.getStem() + FIELD_DELIM + trgMap.get(trgPhrase).getPairFeatStr() + "\n");          
        }
      }
    }

    if (monoWriter != null) {
      monoWriter.close();
    }

    if (addMonoWriter != null) {
      addMonoWriter.close();
    }
  }

  public void saveReorderingTable(String reorderingTableFile) throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(reorderingTableFile), m_encoding));    

    List<Phrase> srcPhraseList = new ArrayList<Phrase>(m_phraseMap.keySet());
    Collections.sort(srcPhraseList, new LexComparator(true));
    
    Map<Phrase, PairProps> trgMap;
    List<Phrase> trgPhraseList;
    
    for (Phrase srcPhrase : srcPhraseList) {
      
      trgMap = m_phraseMap.get(srcPhrase);    
      trgPhraseList = new ArrayList<Phrase>(trgMap.keySet());
      Collections.sort(trgPhraseList, new LexComparator(true));
      
      for (Phrase trgPhrase : trgPhraseList) {
        writer.write(srcPhrase.getStem() + FIELD_DELIM + trgPhrase.getStem() + FIELD_DELIM + trgMap.get(trgPhrase).getOrderFeatStr() + "\n");
      }
    }

    writer.close();
  }
  
  public int processPhraseTableFile(String phraseTableFile, int numLines) throws IOException {
    
    // If we got a new file - set up a new reader for it
    if (!phraseTableFile.equals(m_curFileName)) {
      
      m_curFileName = phraseTableFile;
      if (m_curFileReader != null) {
        m_curFileReader.close();
      }
     
      InputStream is = new FileInputStream(phraseTableFile);
      
      if (phraseTableFile.toLowerCase().endsWith("gz"))
      { is = new GZIPInputStream(is);
      }
      
      m_curFileReader = new BufferedReader(new InputStreamReader(is, m_encoding));
    }

    String line = null, srcStr, trgStr, pairProps;
    Phrase srcPhrase, trgPhrase;
    Map<Phrase, PairProps> trgMap;
    GettableHashSet<Phrase> allTrgPhrases = new GettableHashSet<Phrase>();
    int from, to;
    int numLinesRead = 0;

    m_phraseMap.clear();
    m_numPairs = 0;
    
    while (((numLines < 0) || (numLinesRead < numLines)) &&
           ((line = m_curFileReader.readLine()) != null)) {
      
      numLinesRead++;
      
      if (line.contains(FIELD_DELIM)) {
        
        from = 0;
        to = line.indexOf(FIELD_DELIM);
        srcStr = line.substring(from, to);
        
        from = to + FIELD_DELIM.length();
        to = line.indexOf(FIELD_DELIM, from);
        trgStr = line.substring(from, to);
        
        from = to + FIELD_DELIM.length();
        pairProps = line.substring(from);
        
        (srcPhrase = new Phrase()).init(srcStr, m_caseSensitive);
        
        if (null == (trgMap = m_phraseMap.get(srcPhrase))) {
          srcPhrase.assignId();
          m_phraseMap.put(srcPhrase, trgMap = new HashMap<Phrase, PairProps>());          
        }
        
        (trgPhrase = new Phrase()).init(trgStr, m_caseSensitive);
        
        if (!allTrgPhrases.contains(trgPhrase)) {
          trgPhrase.assignId();
          allTrgPhrases.add(trgPhrase);          
        } else {
          trgPhrase = allTrgPhrases.get(trgPhrase);
        }
        
        assert !trgMap.containsKey(trgPhrase);
        trgMap.put(trgPhrase, new PairProps(pairProps));
        
        m_numPairs++;
      }
    }
    
    return numLinesRead;
  }
  
  public void closePhraseTableFile() throws IOException {
    if (m_curFileReader != null) {
      m_curFileReader.close();
    }
    
    m_curFileReader = null;
    m_curFileName = null;
  }
       
  protected Map<Phrase, Map<Phrase, PairProps>> m_phraseMap;
  protected String m_encoding;
  protected boolean m_caseSensitive;
  protected int m_numPairs;
  protected String m_curFileName;
  protected BufferedReader m_curFileReader;  
  
  public class PairProps {    
    public PairProps(String pairFeatStr) {
      
      String[] strFeats = pairFeatStr.substring(pairFeatStr.lastIndexOf(FIELD_DELIM) + FIELD_DELIM.length()).split("\\s");
      
      int maxFeats = PairFeat.values().length;
      assert strFeats.length <= maxFeats;
      
      m_pairFeatStr = new StringBuilder(pairFeatStr);
      m_dirtyFeatStr = false;
      m_featVals = new String[maxFeats];
       
      for (int i = 0; i < strFeats.length; i++) {
        m_featVals[i] = strFeats[i];
      }
      
      for (int i = strFeats.length; i < maxFeats; i++) {
        m_featVals[i] = null;
      }
    }
    
    public void setPairFeatVal(PairFeat feat, double val) {
      m_featVals[feat.idx] = Double.toString(val);
      m_dirtyFeatStr = true;
    }

    public Double getPairFeatVal(PairFeat feat) {      
      return m_featVals[feat.idx] == null ? null : Double.parseDouble(m_featVals[feat.idx]);
    }
    
    // String up to the first null feature
    public String getPairFeatStr() {
      
      if (m_dirtyFeatStr) {
        m_dirtyFeatStr = false;
        m_pairFeatStr = new StringBuilder(m_pairFeatStr.substring(0, m_pairFeatStr.lastIndexOf(FIELD_DELIM) + FIELD_DELIM.length()));
        
        for (int i = 0; i < PairFeat.values().length; i++) {
          
          if (m_featVals[i] == null) {
            break;
          } else if (i != 0) {
            m_pairFeatStr.append(" ");
          }
          
          m_pairFeatStr.append(m_featVals[i]);
        }        

      }
      
      return m_pairFeatStr.toString();
    }
    
    // String up to the first null feature
    public String getPairFeatStr(PairFeat[] feats) {
      
      StringBuilder pairFeatStr = new StringBuilder(m_pairFeatStr.substring(0, m_pairFeatStr.lastIndexOf(FIELD_DELIM) + FIELD_DELIM.length()));
      boolean first = true;
      
      for (PairFeat feat : feats) {
        if (!first) {
          pairFeatStr.append(" ");
        } 
        pairFeatStr.append(m_featVals[feat.idx] == null ? 0 : m_featVals[feat.idx]);
        first = false;
      }
      
      return pairFeatStr.toString();
    }
    
    public void setBeforeOrderFeatVals(double beforeMono, double beforeSwap, double beforeOutOfOrder) {
      assert beforeMono >= 0 && beforeSwap >= 0 && beforeOutOfOrder >= 0;

      m_beforeOrderFeatStr = new StringBuilder();
      m_beforeOrderFeatStr.append(beforeMono + " ");
      m_beforeOrderFeatStr.append(beforeSwap + " ");
      m_beforeOrderFeatStr.append(beforeOutOfOrder + " ");
    }
    
    public void setAfterOrderFeatVals(double afterMono, double afterSwap, double afterOutOfOrder) {
      assert afterMono >= 0 && afterSwap >= 0 && afterOutOfOrder >= 0;
      
      m_afterOrderFeatStr = new StringBuilder();
      m_afterOrderFeatStr.append(afterMono + " ");
      m_afterOrderFeatStr.append(afterSwap + " ");
      m_afterOrderFeatStr.append(afterOutOfOrder);
    }
    
    public String getOrderFeatStr() {
      
      String orderFeatStr = m_beforeOrderFeatStr != null ? m_beforeOrderFeatStr.toString() : "0.333333 0.333333 0.333333 ";
      orderFeatStr += m_afterOrderFeatStr != null ? m_afterOrderFeatStr.toString() : "0.333333 0.333333 0.333333";
      return orderFeatStr;
    }
    
    public int[][] getForwardAligns() {
      String line = m_pairFeatStr.toString();
      return getAlignment(line.substring(0, line.indexOf(FIELD_DELIM)).trim());
    }

    public int[][] getBackwardAligns() {
      String line = m_pairFeatStr.toString();
      return getAlignment(line.substring(line.indexOf(FIELD_DELIM) + FIELD_DELIM.length(), line.lastIndexOf(FIELD_DELIM)).trim());
    }
    
    protected int[][] getAlignment(String alignStr) {
      String[] strAligns = alignStr.split(" ");
      int[][] aligns = new int[strAligns.length][];
      String[] curAligns;
     
      for (int i = 0; i < strAligns.length; i++) {
        if ("()".equals(strAligns[i])) {
          aligns[i] = null;
        } else {
          curAligns = strAligns[i].substring(1, strAligns[i].length() - 1).split(",");
          aligns[i] = new int[curAligns.length];
          
          for (int j = 0; j < curAligns.length; j++) {
            aligns[i][j] = Integer.parseInt(curAligns[j]);
          }
        }        
      }
      
      return aligns;
    }

    protected String[] m_featVals;
    protected StringBuilder m_pairFeatStr;
    protected boolean m_dirtyFeatStr;
    protected StringBuilder m_beforeOrderFeatStr;
    protected StringBuilder m_afterOrderFeatStr;
  }
}