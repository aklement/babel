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
    PHPENALTY(4);   
  
    private PairFeat(final int idx) {
      this.idx = idx;
    }
    
    public final int idx;
  };
  
  public PhraseTable (String phraseTableFile, String encoding, boolean caseSensitive) throws IOException {
    m_encoding = encoding;
    m_phraseMap = new HashMap<Phrase, Map<Phrase, PairProps>>();
    m_caseSensitive = caseSensitive;
    
    processPhraseTableFile(phraseTableFile);
  }

  public PhraseTable (String phraseTableFile, boolean caseSensitive) throws IOException {
    this(phraseTableFile, DEFAULT_CHARSET, caseSensitive);
  }
  
  public Set<Phrase> getAllSrcPhrases() {
    return m_phraseMap.keySet();
  }

  public int numSrcPhrases() {
    return m_phraseMap.size();
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
  
  public void savePhraseTable(String phraseTableFile) throws IOException {
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(phraseTableFile), m_encoding));    

    List<Phrase> srcPhraseList = new ArrayList<Phrase>(m_phraseMap.keySet());
    Collections.sort(srcPhraseList, new LexComparator(true));
    
    Map<Phrase, PairProps> trgMap;
    List<Phrase> trgPhraseList;
    
    for (Phrase srcPhrase : srcPhraseList) {
      
      trgMap = m_phraseMap.get(srcPhrase);    
      trgPhraseList = new ArrayList<Phrase>(trgMap.keySet());
      Collections.sort(trgPhraseList, new LexComparator(true));
      
      for (Phrase trgPhrase : trgPhraseList) {
        writer.write(srcPhrase.getStem() + FIELD_DELIM + trgPhrase.getStem() + FIELD_DELIM + trgMap.get(trgPhrase).getPairFeatStr() + "\n");
      }
    }

    writer.close();
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
  
  protected void processPhraseTableFile(String phraseTableFile) throws IOException {
    
    InputStream is = new FileInputStream(phraseTableFile);
    
    if (phraseTableFile.toLowerCase().endsWith("gz"))
    { is = new GZIPInputStream(is);
    }
        
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, m_encoding));    
    String line, srcStr, trgStr, pairProps;
    Phrase srcPhrase, trgPhrase;
    Map<Phrase, PairProps> trgMap;
    GettableHashSet<Phrase> allTrgPhrases = new GettableHashSet<Phrase>();
    int from, to;
    
    m_phraseMap.clear();
    
    while ((line = reader.readLine()) != null) {
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
      }
    }

    reader.close();
  }
       
  protected Map<Phrase, Map<Phrase, PairProps>> m_phraseMap;
  protected String m_encoding;
  protected boolean m_caseSensitive;
  
  public class PairProps {    
    public PairProps(String pairFeatStr) {
      m_pairFeatStr = new StringBuilder(pairFeatStr);
    }
    
    public void addPairFeatVal(double val) {
      m_pairFeatStr.append(" " + val);
    }

    public double getPairFeatVal(PairFeat feat) {
      String[] strFeats = m_pairFeatStr.substring(m_pairFeatStr.lastIndexOf(FIELD_DELIM)).split("\\s");
      return Double.parseDouble(strFeats[feat.idx]);
    }
    
    public String getPairFeatStr() {
      return m_pairFeatStr.toString();
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

    protected StringBuilder m_pairFeatStr;
    protected StringBuilder m_beforeOrderFeatStr;
    protected StringBuilder m_afterOrderFeatStr;
  }
}