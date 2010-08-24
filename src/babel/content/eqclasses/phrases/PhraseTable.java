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
  
  public PhraseTable (String phraseTableFile, String encoding) throws IOException {
    m_encoding = encoding;
    m_phraseMap = new HashMap<Phrase, Map<Phrase, PairProps>>();
    
    processPhraseTableFile(phraseTableFile);
  }

  public PhraseTable (String phraseTableFile) throws IOException {
    this(phraseTableFile, DEFAULT_CHARSET); 
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
        writer.write(srcPhrase.getStem() + FIELD_DELIM + trgPhrase.getStem() + FIELD_DELIM + trgMap.get(trgPhrase).getPropStr() + "\n");
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
        
        (srcPhrase = new Phrase()).init(srcStr, false);
        
        if (null == (trgMap = m_phraseMap.get(srcPhrase))) {
          srcPhrase.assignId();
          m_phraseMap.put(srcPhrase, trgMap = new HashMap<Phrase, PairProps>());
        }
        
        (trgPhrase = new Phrase()).init(trgStr, false);
        
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
  
  public class PairProps {    
    public PairProps(String props) {
      m_props = new StringBuilder(props);
    }
    
    public void addFeatureVal(double val) {
      m_props.append(" " + val);
    }

    public String getPropStr() {
      return m_props.toString();
    }
    
    public int[][] getForwardAligns() {
      String line = m_props.toString();
      return getAlignment(line.substring(0, line.indexOf(FIELD_DELIM)).trim());
    }

    public int[][] getBackwardAligns() {
      String line = m_props.toString();
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

    protected StringBuilder m_props;
  }
}