package babel.util.dict;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.content.eqclasses.properties.context.Context;
import babel.content.eqclasses.properties.context.Context.ContextualItem;
import babel.util.misc.RegExFileNameFilter;
import babel.util.misc.FileList;

public class ProbabilisticDictionary
{
  protected static final String DEFAULT_ENCODING = "UTF-8";

  protected ProbabilisticDictionary(String name, Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs)
  {
    m_rand = new Random(1);
    m_map = new HashMap<String, HashMap<String,Double>>();
    m_mapeq = new HashMap<EquivalenceClass, HashMap<EquivalenceClass,Double>>();
    m_name = name;
    m_srcMap = new HashMap<Long, EquivalenceClass>();
    m_trgMap = new HashMap<Long, EquivalenceClass>();
    
  }
  
  public ProbabilisticDictionary(String dictFile, String name, Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs) throws Exception
  {
    this(name, srcEqs, trgEqs);
    
    read(m_map, new InputStreamReader(new FileInputStream(dictFile), DEFAULT_ENCODING));
    construct(srcEqs, trgEqs);

  }
  
  
  public ProbabilisticDictionary(String dictPath, String fileNameRegEx, String name, Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs) throws Exception
  {
    this(name, srcEqs, trgEqs);

    FileList list = new FileList(dictPath, new RegExFileNameFilter(fileNameRegEx));
    list.gather();

    read(m_map, new InputStreamReader(new SequenceInputStream(list), DEFAULT_ENCODING));
    construct(srcEqs, trgEqs);

  }
  
  public String getName()
  {
    return m_name;
  }

  
  public void clear()
  {
    m_map.clear();
  }
  
  public Set<String> getAllSrc()
  {
    return new HashSet<String>(m_map.keySet());
  }
  
  public int size()
  {
    return m_map.size();
  }
  
  public Set<String> getAllTrg()
  {
    HashSet<String> allVals = new HashSet<String>();
    
    for (HashMap<String,Double> valList : m_map.values()) {
    	for (String x : valList.keySet()){
    		allVals.add(x);
    	}
    }
    
    return allVals;
  }

  public HashMap<String,Double> getTrg(String src)
  {
	  return m_map.get(src);
  }
  
  public void write(String fileName) throws Exception
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for(String key : m_map.keySet())
    {
      for (String trg : m_map.get(key).keySet())
      {
        writer.write(key + "\t" + trg + "\t" + m_map.get(key).get(trg).toString());
        writer.newLine();
      }
    }
    
    writer.close();
  }
  
  protected void read(HashMap<String, HashMap<String,Double>> hash, InputStreamReader dictReader) throws Exception
  { 
    BufferedReader reader = new BufferedReader(dictReader);
    String line;
    String[] toks;
    String srcTok, trgTok;
    Double prob;
    
    while (null != (line = reader.readLine()))
    {
      toks = line.split("\\t");
      if (toks.length>2){
        srcTok = toks[0].trim().toLowerCase();
        trgTok = toks[1].trim().toLowerCase();
        prob = Double.parseDouble(toks[2]);
        
        if (isToken(srcTok) && isToken(trgTok))
        {
          if (null == hash.get(srcTok))
          { 
        	  hash.put(srcTok, new HashMap<String,Double>());
          }
    	  hash.get(srcTok).put(trgTok, prob);
        }
      }
    }
    reader.close();
  }
  
 
  
  public void pruneCounts(int numTrans)
  {
    if (numTrans < 0)
    { return;
    }
    
    HashSet<String> toRemove = new HashSet<String>();
    
    for (String name : m_map.keySet())
    {
      if (m_map.get(name).size() >= numTrans)
      { toRemove.add(name);
      }
    }
    
    for (String name : toRemove)
    { m_map.remove(name);
    }
    
    System.out.println("Pruned words with more than " + numTrans + " translations, new size of " + m_name + " is " + m_map.size() + " src entries.");
  }
  
  protected void construct(Set<EquivalenceClass> srcEq, Set<EquivalenceClass> trgEq)
  {
    HashMap<String, EquivalenceClass> srcMap = null;
    HashMap<String, EquivalenceClass> trgMap = null;
    
    // Put src eqs in srcMap, if there are any
    if (srcEq != null) { 
      srcMap = new HashMap<String, EquivalenceClass>();
    
      for (EquivalenceClass eq : srcEq) {
        for (String sWord : eq.getAllWords()) { 
        	assert !srcMap.containsKey(sWord);
          srcMap.put(sWord, eq);
        }
      }
    }

    // Put trg eqs in trgMap, if there are any
    if (trgEq != null) {
      trgMap = new HashMap<String, EquivalenceClass>();
    
      for (EquivalenceClass eq : trgEq) {
        for (String tWord : eq.getAllWords()) { 
          assert !trgMap.containsKey(tWord);
          trgMap.put(tWord, eq);
        }
      }
    }
    
  
    HashMap<EquivalenceClass,Double> tEqSet;
    EquivalenceClass sEq;
    EquivalenceClass tEq;
    
    for (String sDictWord : this.getAllSrc())
    {
      if (srcMap == null) {
        (sEq = new SimpleEquivalenceClass()).init(sDictWord, false);
      } 
      else {
        sEq = srcMap.get(sDictWord); 
      }
           
      if (sEq != null)
      {
        for (String tDictWord : this.m_map.get(sDictWord).keySet())
        {
          
          if (trgMap == null) {
            (tEq = new SimpleEquivalenceClass()).init(tDictWord, false);
          } else {
            tEq = trgMap.get(tDictWord);
          }
          
          if (tEq != null)
          {
            if (null == (tEqSet = m_mapeq.get(sEq)))
            { m_mapeq.put(sEq, new HashMap<EquivalenceClass,Double>());
            tEqSet=new HashMap<EquivalenceClass,Double>();
            }
            
            tEqSet.put(tEq,m_map.get(sDictWord).get(tDictWord));
            
            m_srcMap.put(sEq.getId(), sEq);
            m_trgMap.put(tEq.getId(), tEq);
          }
        }
      }
      //else{
    //	  System.out.println("WARNING: source word "+sDictWord+" not found.");
      //}
    }
  }
  
  
  // Note their could be repeats if we got the same translation through different contextualsrc words:
  // Simply increment counts by probabilities
  public List<ContextualItem> translateContext(EquivalenceClass eq)
  {    
    Context oldContext = (Context)eq.getProperty(Context.class.getName());
    LinkedList<ContextualItem> newCis = new LinkedList<ContextualItem>();
    
    if (oldContext != null)
    {
      Collection<ContextualItem> oldCis = oldContext.getContextualItems();
      Long srcEqId;
      HashMap<EquivalenceClass,Double> translations;
      EquivalenceClass srcEq;
      Double prob;
      Double updatedCorpusCount;
      Double updatedContextCount;
      for (ContextualItem oldCi : oldCis)
      {
    	  srcEqId = oldCi.getContextEqId();
          //System.out.println("Old context: "+oldCi.toString());
        
        // Look up src eq and then all of its translations
        if ((null != (srcEq = m_srcMap.get(srcEqId))) && (null != (translations = m_mapeq.get(srcEq))))
        {
        	// Add a contextual item for each translation keeping the src count
          for (EquivalenceClass trgEq : translations.keySet())
          {
          	//System.out.println("Found a translation: "+trgEq.toString());
        	//Get probability
        	prob=translations.get(trgEq);
        	// Context context, Long contEqId, long count
        	// Change context count to true-context-count*translation-probability
        	updatedCorpusCount = prob * oldCi.getCorpusCount();
        	updatedContextCount = prob * oldCi.getContextCount();
        	newCis.add(new ContextualItem(null, trgEq.getId(), updatedCorpusCount.longValue(), updatedContextCount.longValue()));
          }
        }
      }
    }
    
    return newCis;
  }
  
  
  
  public String toString()
  {
    return "SimpleDictionary [" + m_name + "] contains " + m_map.keySet().size() + " source entries.";
  }

  protected boolean isToken(String str)
  {
    return !str.matches(".*[,0-9_\\*\\.]+.*");
  }

  protected HashMap<String, HashMap<String,Double>> m_map;
  protected HashMap<EquivalenceClass, HashMap<EquivalenceClass,Double>> m_mapeq;
  protected Random m_rand;
  protected String m_name;
  protected HashMap<Long, EquivalenceClass> m_srcMap;
  protected HashMap<Long, EquivalenceClass> m_trgMap;
  
  public static class DictHalves {
    public DictHalves(String srcDictFile, String trgDictFile) {
      
      assert srcDictFile != null && trgDictFile != null && !srcDictFile.equals(trgDictFile);
      
      this.srcDictFile = srcDictFile;
      this.trgDictFile = trgDictFile;
    }
    
    public String srcDictFile, trgDictFile;
  }
  
  public class DictPair
  {    
    public DictPair(String nameDict, String nameRest)
    {
      dict = new SimpleDictionary(nameDict);
      rest = new SimpleDictionary(nameRest);
    }
    
    public SimpleDictionary dict;
    public SimpleDictionary rest;
  }
}
