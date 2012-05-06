package babel.util.dict;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.Set;

import babel.util.misc.RegExFileNameFilter;
import babel.util.misc.FileList;

public class SimpleDictionary
{
  protected static final String DEFAULT_ENCODING = "UTF-8";

  protected SimpleDictionary(String name)
  {
    m_rand = new Random(1);
    m_map = new HashMap<String, HashSet<String>>();
    m_name = name;
  }

  public SimpleDictionary(DictHalves dictFiles, String name) throws Exception
  {
    this(name);
    
    read(m_map, new InputStreamReader(new FileInputStream(dictFiles.srcDictFile), DEFAULT_ENCODING), new InputStreamReader(new FileInputStream(dictFiles.trgDictFile), DEFAULT_ENCODING));
  }
  
  public SimpleDictionary(String dictFile, String name) throws Exception
  {
    this(name);
    
    read(m_map, new InputStreamReader(new FileInputStream(dictFile), DEFAULT_ENCODING));
  }
  
  public SimpleDictionary(String dictPath, String fileNameRegEx, String name) throws Exception
  {
    this(name);

    FileList list = new FileList(dictPath, new RegExFileNameFilter(fileNameRegEx));
    list.gather();

    read(m_map, new InputStreamReader(new SequenceInputStream(list), DEFAULT_ENCODING));
  }
  
  public String getName()
  {
    return m_name;
  }

  public DictPair splitPercent(String nameDict, String nameRest, double part) throws Exception 
  {
    if (part < 0 || part > 1)
    { throw new IllegalArgumentException();
    }
    
    return splitPart(nameDict, nameRest, (int) (m_map.size() * part));
  }
  
  /**
   * Note: if partSize is larger than the SimpleDictionary - entire dict is added to
   * the first dict in the pair.
   */
  public DictPair splitPart(String nameDict, String nameRest, int partSize) throws Exception 
  {
    if (partSize < 0)
    { throw new IllegalArgumentException();
    }
    
    int part = Math.min(m_map.size(), partSize);
    String nextSrc;
    LinkedList<String> keys = new LinkedList<String>();
    DictPair pair = new DictPair(nameDict, nameRest);
    
    keys.addAll(m_map.keySet());
    
    for (int i = 0; i < part; i++)
    {
      nextSrc = keys.remove(m_rand.nextInt(keys.size()));
      pair.dict.m_map.put(nextSrc, m_map.get(nextSrc));
    }
    
    for (int i = 0; i < keys.size(); i++)
    {
      nextSrc = keys.get(i);
      pair.rest.m_map.put(nextSrc, m_map.get(nextSrc));
    }
    
    return pair;
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
    
    for (HashSet<String> valList : m_map.values())
    { allVals.addAll(valList);
    }
    
    return allVals;
  }

  public Set<String> getTrg(String src)
  {
    return m_map.get(src);
  }
  
  public void write(String fileName) throws Exception
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for(String key : m_map.keySet())
    {
      for (String val : m_map.get(key))
      {
        writer.write(key + "\t" + val);
        writer.newLine();
      }
    }
    
    writer.close();
  }
  
  protected void read(HashMap<String, HashSet<String>> hash, InputStreamReader dictReader) throws Exception
  { 
    BufferedReader reader = new BufferedReader(dictReader);
    String line;
    String[] toks;
    HashSet<String> transSet;
    String srcTok, trgTok;
    
    while (null != (line = reader.readLine()))
    {
      toks = line.split("\\s");
      srcTok = toks[0].toLowerCase();
      trgTok = toks[1].toLowerCase();
      
      if (isToken(srcTok) && isToken(trgTok))
      {
        if (null == (transSet = hash.get(srcTok)))
        { hash.put(srcTok, transSet = new HashSet<String>());
        }

        transSet.add(trgTok);
      }
    }
    reader.close();
  }
  
  protected void read(HashMap<String, HashSet<String>> hash, InputStreamReader srcDictReader, InputStreamReader trgDictReader) throws Exception { 
    
    BufferedReader srcReader = new BufferedReader(srcDictReader);
    BufferedReader trgReader = new BufferedReader(trgDictReader);

    String srcTok, trgTok;
    HashSet<String> transSet;
    
    while (null != (srcTok = srcReader.readLine()) && null != (trgTok = trgReader.readLine()))
    {
      srcTok = srcTok.trim().toLowerCase();
      trgTok = trgTok.trim().toLowerCase();
       
      if (isToken(srcTok) && isToken(trgTok))
      {
        if (null == (transSet = hash.get(srcTok)))
        { hash.put(srcTok, transSet = new HashSet<String>());
        }

        transSet.add(trgTok);
      }
    }
    
    srcReader.close();
    trgReader.close();
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
  
  public String toString()
  {
    return "SimpleDictionary [" + m_name + "] contains " + m_map.size() + " source entries.";
  }

  protected boolean isToken(String str)
  {
    return !str.matches(".*[,0-9_\\*\\.]+.*");
  }

  protected HashMap<String, HashSet<String>> m_map;
  protected Random m_rand;
  protected String m_name;
  
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
