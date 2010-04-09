package babel.util.dict;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.io.SequenceInputStream;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
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

import babel.ranking.EquivClassPairsRanking;
import babel.ranking.EquivClassPairsRanking.ScoredPair;

import babel.util.misc.RegExFileNameFilter;
import babel.util.misc.FileList;

public class Dictionary
{
  protected static final Comparator<EquivalenceClass> OVERLAP_COMPARATOR = new OverlapComparator();
  protected static final String DEFAULT_ENCODING = "UTF-8";
  protected static final Log LOG = LogFactory.getLog(Dictionary.class);

  public Dictionary(Class<? extends EquivalenceClass> classFrom, Class<? extends EquivalenceClass> classTo, String name)
  {
    m_classFrom = classFrom;
    m_classTo = classTo;
    m_rand = new Random(1); 
    m_map = new HashMap<EquivalenceClass, List<EquivalenceClass>>();
    m_name = name;
  }
  
  public Dictionary(String dictFile, Class<? extends EquivalenceClass> classFrom, Class<? extends EquivalenceClass> classTo, String name, boolean filterRomanizedTrg) throws Exception
  {
    this(classFrom, classTo, name);
    
    read(m_map, new InputStreamReader(new FileInputStream(dictFile), DEFAULT_ENCODING), filterRomanizedTrg);
  }
  
  public Dictionary(String dictPath, String fileNameRegEx, Class<? extends EquivalenceClass> classFrom, Class<? extends EquivalenceClass> classTo, String name, boolean filterRomanizedTrg) throws Exception
  {
    this(classFrom, classTo, name);

    FileList list = new FileList(dictPath, new RegExFileNameFilter(fileNameRegEx));
    list.gather();

    read(m_map, new InputStreamReader(new SequenceInputStream(list), DEFAULT_ENCODING), filterRomanizedTrg);
  }
  
  public Dictionary clone()
  {
    Dictionary dict = new Dictionary(m_classFrom, m_classTo, m_name);
    
    for (EquivalenceClass eqSrc : m_map.keySet())
    { dict.m_map.put(eqSrc, new LinkedList<EquivalenceClass>(m_map.get(eqSrc)));
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
  
  public List<EquivalenceClass> getTranslations(EquivalenceClass srcEq)
  {
    return m_map.containsKey(srcEq) ? new LinkedList<EquivalenceClass>(m_map.get(srcEq)) : null;
  }
  
  public EquivalenceClass getRandomTranslation(EquivalenceClass srcEq)
  {
    List<EquivalenceClass> allTrans = m_map.get(srcEq);
    EquivalenceClass trans = null;
    
    if (allTrans != null && allTrans.size() > 0)
    {
      trans = allTrans.get(m_rand.nextInt(allTrans.size()));
    }
   
    return trans;
  }
  
  public static void main(String[] args) throws Exception
  {
    String dictPath = "/Users/aklement/Resources/Dictionaries/Chris/dict";
    
    Dictionary d = new Dictionary(dictPath, ".*\\.turkish", SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "Test", false);

    SimpleEquivalenceClass from = new SimpleEquivalenceClass();
    from.init(-1, "test", false);
    
    SimpleEquivalenceClass to = new SimpleEquivalenceClass();
    to.init(-1, "a", false);    
    d.checkTranslation(from, to);
  }
  
  public boolean checkTranslation(EquivalenceClass srcEq, EquivalenceClass trgEq)
  {
    List<EquivalenceClass> eqs;

    return (((eqs = m_map.get(srcEq)) != null) && (Collections.binarySearch(eqs, trgEq, OVERLAP_COMPARATOR) >= 0));
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
  public void augment(Dictionary dict, boolean overwrite)
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
    
    for (List<EquivalenceClass> valList : m_map.values())
    { allVals.addAll(valList);
    }
    
    return allVals;
  }

  public boolean hasTranslations(EquivalenceClass key)
  {
    List<EquivalenceClass> trans;
    
    return ((key != null) && ((trans = m_map.get(key)) != null) && trans.size() > 0);
  }
  
  public void remove(EquivalenceClass key)
  {
    m_map.remove(key);
  }

  /**
   * Note: Does NOT overwrite an existing pair. 
   */
  public void add(EquivalenceClass key, List<EquivalenceClass> vals)
  {
    if (!m_map.containsKey(key))
    {
      Collections.sort(vals, OVERLAP_COMPARATOR);
      m_map.put(key, vals);
    }
  }

  public void add(EquivalenceClass key, EquivalenceClass val)
  {
    List<EquivalenceClass> vals = m_map.get(key);
    int idx;
    
    if (vals == null)
    {
      vals = new LinkedList<EquivalenceClass>();
      vals.add(val);
      m_map.put(key, vals);
    }
    else if ((idx = Collections.binarySearch(vals, val, OVERLAP_COMPARATOR)) < 0)
    {
      vals.add((-(idx) - 1), val);
    }
  }
  
  public void addAll(EquivClassPairsRanking ranking)
  {
    List<ScoredPair> pairs = ranking.getScoredPairs();
    
    for (ScoredPair pair : pairs)
    {
      add(pair.getSrcEq(), pair.getTrgEq());
    }
  }
  
  public void write(String fileName) throws Exception
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for(EquivalenceClass key : m_map.keySet())
    {
      for (EquivalenceClass val : m_map.get(key))
      {
        writer.write(key + "\t" + val);
        writer.newLine();
      }
    }
    
    writer.close();
  }
  
  protected void read(HashMap<EquivalenceClass, List<EquivalenceClass>> hash, InputStreamReader dictReader, boolean filterRomanizedTrg) throws Exception
  { 
    BufferedReader reader = new BufferedReader(dictReader);
    LinkedList<ListEntry> entries = new LinkedList<ListEntry>();
    String line;
    String[] toks;
    ListEntry listEntry = null;
    List<EquivalenceClass> list;
    EquivalenceClass eq;
    int idx;
    Pattern romanChars = Pattern.compile("[a-zA-Z]");
    
    while (null != (line = reader.readLine()))
    {
      toks = line.split("\\s");
      
      if (toks != null && toks.length > 1 && isToken(toks[0]) && isToken(toks[1]) && !(filterRomanizedTrg && romanChars.matcher(toks[1]).find()))
      {
        listEntry = new ListEntry();
        listEntry.key = m_classFrom.newInstance();
        listEntry.key.init(-1, toks[0], false);
        
        // Add a key and get a list for translations
        if ((idx = Collections.binarySearch(entries, listEntry, listEntry)) >= 0)
        {
          // Merge the equivalence classes
          entries.get(idx).key.merge(listEntry.key);
           
          // Get a list of translations
          list = entries.get(idx).allVals;
          
          // System.out.print("Found " + toks[0]);
        }
        else
        {
          // Add the new entry
          entries.add((-(idx) - 1), listEntry);
          
          // Create a new list for it
          list = listEntry.allVals = new LinkedList<EquivalenceClass>(); 
          
          // System.out.print("Added " + toks[0]);
        }
        
        eq = m_classTo.newInstance();
        eq.init(-1, toks[1], false);
        
        // Now check for the second eq class
        if ((idx = Collections.binarySearch(list, eq, OVERLAP_COMPARATOR)) >= 0)
        {
          // Merge the equivalence classes
          list.get(idx).merge(eq);
        }
        else
        {
          list.add((-(idx) - 1), eq);
        }
        
        // System.out.println(" -> " + toks[1]);

      }
    }
    
    reader.close();

    for (ListEntry entry : entries)
    { hash.put(entry.key, entry.allVals); 
    }
  }
  
  public List<DictLookup> translateContext(EquivalenceClass eq)
  {
    LinkedList<DictLookup> tr = new LinkedList<DictLookup>();
    
    Context c = (Context)eq.getProperty(Context.class.getName());
    
    if (c != null)
    {
      Collection<ContextualItem> clist = c.getContext();
      List<EquivalenceClass> trans;
      
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
  
  public PrecRecall precRecall(Set<EquivalenceClass> discSrcEqs, Dictionary other, boolean verbose)
  {    
    double numCorrect = 0;
    double prec, recall;
    DecimalFormat df = new DecimalFormat("0.00");

    /* Items we wanted to retrieve which also appear in gold dictionary. */
    HashSet<EquivalenceClass> goldSrcEqs = new HashSet<EquivalenceClass>(m_map.keySet());
    goldSrcEqs.retainAll(discSrcEqs);
    
    HashSet<EquivalenceClass> otherSrcEqs = new HashSet<EquivalenceClass>(other.m_map.keySet());
    otherSrcEqs.retainAll(goldSrcEqs);

    for (EquivalenceClass srcEq : otherSrcEqs)
    {
      if (commonTranslation(m_map.get(srcEq), other.m_map.get(srcEq)))
      { 
        numCorrect++;
        if (verbose)
        { LOG.info("Same " + other.m_name + "/" + m_name  + ": "+ srcEq.toString() + " <-> " + other.m_map.get(srcEq).toString() +  " / " + m_map.get(srcEq).toString());
        }
      }
      else
      { 
        if (verbose)
        { LOG.info("Different " + other.m_name + "/" + m_name  + ": "+ srcEq.toString() + " <-> " + other.m_map.get(srcEq).toString() +  " / " + m_map.get(srcEq).toString());
        }
      }
    }
    
    prec = (double)numCorrect / (double)otherSrcEqs.size();
    recall = (double)numCorrect / (double)goldSrcEqs.size();
    
    LOG.info("Precision: " + df.format(prec * 100) + "%, recall: " + df.format(recall * 100) + "%");
    
    return new PrecRecall(prec, recall);
  }
  
  /**
   * Computes the ration of words in the given dictionary which were found to
   * have correct translations according to this dictionary.  If the suplied
   * dictionary has a list of translations - any one of them would mean a match.
   */
  public double accuracy(Set<EquivalenceClass> srcEqs, Dictionary other, boolean details)
  {
    double numCommonKeys = 0;
    double numTranslations = 0;
    
    if (details)
    {
      System.out.println("Computing accuracy gold dictionary " + m_name + " (size " + m_map.size() + "), and dictionary " + other.m_name + " (size " + other.m_map.size() + ").");
    }

    for (EquivalenceClass key : other.m_map.keySet())
    {
      if (m_map.containsKey(key))
      {
        numCommonKeys++;
        
        if (commonTranslation(m_map.get(key), other.m_map.get(key)))
        { numTranslations++;
        }
        else if (details)
        {
          System.out.println("Different " + other.m_name + "/" + m_name  + ": "+ key.toString() + " <-> " + other.m_map.get(key).toString() +  " / " + m_map.get(key).toString());
        }
      }
    }

    System.out.println("Accuracy " + other.m_name + "/" + m_name + ":" + 100 * numTranslations / numCommonKeys + "% based on " + numCommonKeys + " common source words.");
    
    return numTranslations / numCommonKeys;
  }
  
  public void pruneCounts(int numTrans)
  {
    if (numTrans < 0)
    { return;
    }
    
    List<EquivalenceClass> toRemove = new LinkedList<EquivalenceClass>();
    
    for (EquivalenceClass name : m_map.keySet())
    {
      if (m_map.get(name).size() >= numTrans)
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
    return "Dictionary " + m_name + " contains " + m_map.size() + " source language entries.";
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
  protected HashMap<EquivalenceClass, List<EquivalenceClass>> m_map;
  protected Random m_rand;
  protected String m_name;
  
  static class ListEntry implements Comparator<ListEntry>
  {
    public ListEntry()
    {
      allVals = new LinkedList<EquivalenceClass>();
    }

    public int compare(ListEntry entry1, ListEntry entry2)
    {
      return OVERLAP_COMPARATOR.compare(entry1.key, entry2.key);
    }
    
    public String toString()
    {
      StringBuilder bld = new StringBuilder();
      
      bld.append(key.toString());
      bld.append(" : ");
      bld.append(allVals.toString());
      
      return bld.toString();
    }
    
    protected EquivalenceClass key;
    protected LinkedList<EquivalenceClass> allVals;
  }
  
  public class PrecRecall
  {
    public PrecRecall(double prec, double rec)
    {
      precision = prec;
      recall = rec;
    }
    
    public double precision;
    public double recall;
  }
  
  public class DictPair
  {    
    public DictPair(String nameDict, String nameRest)
    {
      dict = new Dictionary(m_classFrom, m_classTo, nameDict);
      rest = new Dictionary(m_classFrom, m_classTo, nameRest);
    }
    
    public Dictionary dict;
    public Dictionary rest;
  }
  
  public class DictLookup
  {
    public DictLookup(Context.ContextualItem srcContItem, List<EquivalenceClass> trgEqClasses)
    {
      this.srcContItem = srcContItem;
      this.trgEqClasses = new ArrayList<EquivalenceClass>(trgEqClasses);
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
    public List<EquivalenceClass> trgEqClasses;
  }
}
