package babel.util.dict;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.content.eqclasses.comparators.OverlapComparator;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Context.ContextualItem;
import babel.ranking.EquivClassCandRanking;
import babel.ranking.EquivClassPairsRanking;
import babel.ranking.EquivClassPairsRanking.ScoredPair;

import babel.util.misc.FileList;
import babel.util.misc.RegExFileNameFilter;

public class ScoredDictionary
{
  protected static final Comparator<EquivalenceClass> OVERLAP_COMPARATOR = new OverlapComparator();
  protected static final String DEFAULT_ENCODING = "UTF-8";
  protected static final Log LOG = LogFactory.getLog(ScoredDictionary.class);

  public ScoredDictionary(Class<? extends EquivalenceClass> classFrom, Class<? extends EquivalenceClass> classTo, boolean smallerScoresAreBetter, int maxNumTrans, String name)
  {
    m_classFrom = classFrom;
    m_classTo = classTo;
    m_rand = new Random(1);
    m_map = new HashMap<EquivalenceClass, EquivClassCandRanking>();
    m_name = name;
    m_smallerScoresAreBetter = smallerScoresAreBetter;
    m_maxNumTrans = maxNumTrans;
  }
  
  public ScoredDictionary(String dictFile, Class<? extends EquivalenceClass> classFrom, Class<? extends EquivalenceClass> classTo, boolean smallerScoresAreBetter, int maxNumTrans, String name, boolean filterRomanizedTrg) throws Exception
  {
    this(classFrom, classTo, smallerScoresAreBetter, maxNumTrans, name);
    
    read(m_map, new InputStreamReader(new FileInputStream(dictFile), DEFAULT_ENCODING), filterRomanizedTrg);
  }
  
  public ScoredDictionary(String dictPath, String fileNameRegEx, Class<? extends EquivalenceClass> classFrom, Class<? extends EquivalenceClass> classTo, boolean smallerScoresAreBetter, int maxNumTrans, String name, boolean filterRomanizedTrg) throws Exception
  {
    this(classFrom, classTo, smallerScoresAreBetter, maxNumTrans, name);

    FileList list = new FileList(dictPath, new RegExFileNameFilter(fileNameRegEx));
    list.gather();

    read(m_map, new InputStreamReader(new SequenceInputStream(list), DEFAULT_ENCODING), filterRomanizedTrg);
  }
  
  public ScoredDictionary clone()
  {
    ScoredDictionary dict = new ScoredDictionary(m_classFrom, m_classTo, m_smallerScoresAreBetter, m_maxNumTrans, m_name);
    
    for (EquivalenceClass eqSrc : m_map.keySet())
    { dict.m_map.put(eqSrc, m_map.get(eqSrc).clone());
    }
    
    return dict;
  }
  
  public void setName(String name)
  {
    m_name = name;
  }

  public String getName()
  {
    return m_name;
  }
  
  public Set<EquivalenceClass> getTranslations(EquivalenceClass srcEq)
  {
    return m_map.containsKey(srcEq) ? new HashSet<EquivalenceClass>(m_map.get(srcEq).getCandidates()) : null;
  }
  
  public int numInTopK(EquivalenceClass srcEq, Collection<EquivalenceClass> cands, int k)
  {
    EquivClassCandRanking ranking = m_map.get(srcEq);
    return ranking == null ? 0 : ranking.numInTopK(cands, k);
  }
  
  public EquivalenceClass getRandomTranslation(EquivalenceClass srcEq)
  {
    return m_map.get(srcEq).getRandomCandidate();
  }
  
  public static void main(String[] args) throws Exception
  {
    String dictPath = "/Users/aklement/Resources/Dictionaries/Chris/dict";
    
    ScoredDictionary d = new ScoredDictionary(dictPath, ".*\\.turkish", SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, false, -1, "Test", false);

    SimpleEquivalenceClass from = new SimpleEquivalenceClass();
    from.init(-1, "test", false);
    
    SimpleEquivalenceClass to = new SimpleEquivalenceClass();
    to.init(-1, "a", false);    
    d.checkTranslation(from, to);
  }
  
  public boolean checkTranslation(EquivalenceClass srcEq, EquivalenceClass trgEq)
  {
    EquivClassCandRanking cands;

    return ((cands = m_map.get(srcEq)) != null) && cands.containsCandidate(trgEq);
  }

  public DictPair splitPercent(String nameDict, String nameRest, double part) 
  {
    if (part < 0 || part > 1)
    { throw new IllegalArgumentException();
    }
    
    return splitPart(nameDict, nameRest, (int) (m_map.size() * part));
  }
  
  /**
   * Note: if partSize is larger than the dictionary - entire dict is added to
   * the first dict in the pair.
   */
  public DictPair splitPart(String nameDict, String nameRest, int partSize) 
  {
    if (partSize < 0)
    { throw new IllegalArgumentException();
    }
    
    int part = Math.min(m_map.size(), partSize);
    EquivalenceClass nextEq;
    LinkedList<EquivalenceClass> keys = new LinkedList<EquivalenceClass>();
    DictPair pair = new DictPair(nameDict, nameRest);
    
    keys.addAll(m_map.keySet());
    
    for (int i = 0; i < part; i++)
    {
      nextEq = keys.remove(m_rand.nextInt(keys.size()));
      pair.dict.m_map.put(nextEq, m_map.get(nextEq));
    }
    
    for (int i = 0; i < keys.size(); i++)
    {
      nextEq = keys.get(i);
      pair.rest.m_map.put(nextEq, m_map.get(nextEq));
    }
    
    return pair;
  }
  
  public void clear()
  {
    m_map.clear();
  }
  
  // TODO: HERE and in add() - should the lists be combined?
  /**
   * Note: Does NOT replace existing values.
   */
  public void augment(ScoredDictionary dict, boolean overwrite)
  {
    for (EquivalenceClass eq : dict.m_map.keySet())
    { 
      if (overwrite || !m_map.containsKey(eq))
      { m_map.put(eq, dict.m_map.get(eq));
      }
    }
  }
  
  public boolean containsKey(EquivalenceClass key)
  {
    return m_map.containsKey(key);
  }
  
  public Set<EquivalenceClass> getAllKeys()
  {
    return new HashSet<EquivalenceClass>(m_map.keySet());
  }
  
  public int size()
  {
    return m_map.size();
  }
  
  public Set<EquivalenceClass> getAllVals()
  {
    HashSet<EquivalenceClass> allVals = new HashSet<EquivalenceClass>();
    
    for (EquivClassCandRanking cands : m_map.values())
    { allVals.addAll(cands.getCandidates());
    }
    
    return allVals;
  }

  public boolean hasTranslations(EquivalenceClass key)
  {    
    return ((key != null) && (m_map.containsKey(key)));
  }
  
  public void remove(EquivalenceClass key)
  {
    m_map.remove(key);
  }

  public void add(EquivalenceClass key, EquivalenceClass val, double score)
  {
    EquivClassCandRanking cands = m_map.get(key);
    
    if (cands == null)
    { m_map.put(key, cands = new EquivClassCandRanking(key, m_maxNumTrans, m_smallerScoresAreBetter));
    }

    cands.add(val, score);
  }
  
  public void addAll(EquivClassPairsRanking ranking)
  {
    List<ScoredPair> pairs = ranking.getScoredPairs();
    
    for (ScoredPair pair : pairs)
    { add(pair.getSrcEq(), pair.getTrgEq(), pair.getScore());
    }
  }
  
  public void write(String fileName) throws Exception
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    LinkedHashMap<EquivalenceClass, Double> scoredCands;
    
    for (EquivalenceClass key : m_map.keySet())
    {
      scoredCands = m_map.get(key).getOrderedScoredCandidates();
      
      for (EquivalenceClass cand : scoredCands.keySet())      
      {
        writer.write(key + "\t" + cand + "\t" + scoredCands.get(cand));
        writer.newLine();
      }
    }
    
    writer.close();
  }
  
  protected void read(HashMap<EquivalenceClass, EquivClassCandRanking> hash, InputStreamReader dictReader, boolean filterRomanizedTrg) throws Exception
  { 
    BufferedReader reader = new BufferedReader(dictReader);
    String line;
    String[] toks;    
    EquivalenceClass key, cand;
    double score;
    EquivClassCandRanking cands;
    
    Pattern romanChars = Pattern.compile("[a-zA-Z]");
    
    m_map.clear();
    
    while (null != (line = reader.readLine()))
    {
      toks = line.split("\\s");
      
      if (toks != null && toks.length > 2 && isToken(toks[0]) && isToken(toks[1]) && !(filterRomanizedTrg && romanChars.matcher(toks[1]).find()))
      {
        key = m_classFrom.newInstance();
        key.init(-1, toks[0], false);        

        cand = m_classTo.newInstance();
        cand.init(-1, toks[1], false);

        score = Double.parseDouble(toks[2]);
        
        if (null == (cands = m_map.get(key)))
        { m_map.put(key, cands = new EquivClassCandRanking(key, m_maxNumTrans, m_smallerScoresAreBetter));
        }
        
        cands.add(cand, score);
      }
    }
    
    reader.close();
  }
  
  public List<DictLookup> translateContext(EquivalenceClass eq)
  {
    LinkedList<DictLookup> tr = new LinkedList<DictLookup>();
    
    Context c = (Context)eq.getProperty(Context.class.getName());
    
    if (c != null)
    {
      Collection<ContextualItem> clist = c.getContext();
      EquivClassCandRanking trans;
      
      if (clist != null)
      {
        for (ContextualItem citem : clist)
        {
          if (null != (trans = m_map.get(citem.getContextEq())))
          {
            tr.add(new DictLookup(citem, trans));
          }
        }
      }
    }
    
    return tr;
  }
      
  public void pruneCounts(int numTrans)
  {
    if (numTrans < 0)
    { return;
    }
    
    List<EquivalenceClass> toRemove = new LinkedList<EquivalenceClass>();
    
    for (EquivalenceClass name : m_map.keySet())
    {
      if (m_map.get(name).numCandidates() >= numTrans)
      { toRemove.add(name);
      }
    }
    
    for (EquivalenceClass name : toRemove)
    { m_map.remove(name);
    }
    
    System.out.println("Pruned words with more than " + numTrans + " translations, new size of " + m_name + " is " + m_map.size() + ".");
  }
  
  public String toString()
  {
    return "ScoredDictionary " + m_name + " contains " + m_map.size() + " source language entries.";
  }

  protected boolean commonTranslation(List<EquivalenceClass> listThis, List<EquivalenceClass> listOther)
  {
    boolean match = false;
    
    for (EquivalenceClass eq : listThis)
    {
      if (Collections.binarySearch(listOther, eq, OVERLAP_COMPARATOR) >= 0)
      {
        match = true;
        break;
      }
    }
    
    return match;
  }
  
  protected boolean isToken(String str)
  {
    return !str.matches(".*[,0-9_\\*\\.]+.*");
  }

  protected Class<? extends EquivalenceClass> m_classFrom;
  protected Class<? extends EquivalenceClass> m_classTo;  
  protected HashMap<EquivalenceClass, EquivClassCandRanking> m_map;
  protected boolean m_smallerScoresAreBetter;
  protected int m_maxNumTrans;
  protected Random m_rand;
  protected String m_name;
  
  public class DictPair
  {    
    public DictPair(String nameDict, String nameRest)
    {
      dict = new ScoredDictionary(m_classFrom, m_classTo, m_smallerScoresAreBetter, m_maxNumTrans, nameDict);
      rest = new ScoredDictionary(m_classFrom, m_classTo, m_smallerScoresAreBetter, m_maxNumTrans, nameRest);
    }
    
    public ScoredDictionary dict;
    public ScoredDictionary rest;
  }
  
  public class DictLookup
  {
    public DictLookup(Context.ContextualItem srcContItem, EquivClassCandRanking trgEqClasses)
    {
      this.srcContItem = srcContItem;
      this.trgEqClasses = trgEqClasses;
    }
    
    public String toString()
    {
      StringBuilder strBld = new StringBuilder();

      strBld.append(srcContItem.getContextEq().toString());
      strBld.append(": ");
      strBld.append(trgEqClasses);
      
      return strBld.toString();
    }
    
    public Context.ContextualItem srcContItem;
    public EquivClassCandRanking trgEqClasses;
  }
}
