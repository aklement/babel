package babel.util.dict;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.content.eqclasses.properties.context.Context;
import babel.content.eqclasses.properties.context.Context.ContextualItem;

public class Dictionary
{
  /**
   * Create a Dictionary object from a SimpleDictionary.  Will keep only 
   * equivalence classes in srcEqs, and trgEqs. If either of the two sets is
   * null, will create a SimpleEquivalenceClass for (src or trg, respectively)
   * entry in SimpleDictionary.
   */
  public Dictionary(Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs, SimpleDictionary simpleDict, String name)
  {
    m_name = name;
    m_map = new HashMap<EquivalenceClass, HashSet<EquivalenceClass>>();
    m_srcMap = new HashMap<Long, EquivalenceClass>();
    m_trgMap = new HashMap<Long, EquivalenceClass>();
    
    costruct(srcEqs, trgEqs, simpleDict);
  }

  /**
   * Create a Dictionary object from a SimpleDictionary.  Will keep only 
   * equivalence classes in srcEqs, and create a SimpleEquivalenceClass for
   * each target word.
   */
  public Dictionary(Set<EquivalenceClass> srcEqs, SimpleDictionary simpleDict, String name) {
    this(srcEqs, null, simpleDict, name);
  }
  
  public void retainAllSrc(Set<EquivalenceClass> srcEqs)
  {
    HashMap<EquivalenceClass, HashSet<EquivalenceClass>> oldMap = m_map;
    HashMap<Long, EquivalenceClass> oldSrcMap = m_srcMap;
    
    m_map = new HashMap<EquivalenceClass, HashSet<EquivalenceClass>>();
    m_srcMap = new HashMap<Long, EquivalenceClass>();
    m_trgMap.clear();
    
    EquivalenceClass otherSrcEq;

    for (EquivalenceClass sEq : srcEqs)
    {
      if (null != (otherSrcEq = oldSrcMap.get(sEq.getId())))
      { addTranslation(otherSrcEq, oldMap.get(otherSrcEq));
      }
    } 
  }

  public void removeAllSrc(Set<EquivalenceClass> srcEqs)
  {    
    HashMap<EquivalenceClass, HashSet<EquivalenceClass>> oldMap = m_map;
    
    m_map = new HashMap<EquivalenceClass, HashSet<EquivalenceClass>>();
    m_srcMap.clear();
    m_trgMap.clear(); 
    
    HashSet<Long> srcIds = new HashSet<Long>();
      
    for (EquivalenceClass sEq : srcEqs)
    { srcIds.add(sEq.getId());
    }
      
    for (EquivalenceClass otherSrcEq : oldMap.keySet())
    {
      if (!srcIds.contains(otherSrcEq.getId()))
      { addTranslation(otherSrcEq, oldMap.get(otherSrcEq));          
      }
    }
  }
  
  protected void addTranslation(EquivalenceClass srcEq, Set<EquivalenceClass> trgEqSet)
  {
    HashSet<EquivalenceClass> ourTrgEqSet;
    
    m_srcMap.put(srcEq.getId(), srcEq);
        
    if (null == (ourTrgEqSet = m_map.get(srcEq)))
    { m_map.put(srcEq, ourTrgEqSet = new HashSet<EquivalenceClass>());
    }
    
    for (EquivalenceClass tEq : trgEqSet)
    {
      ourTrgEqSet.add(tEq);        
      m_trgMap.put(tEq.getId(), tEq);
    }
  }

  protected void costruct(Set<EquivalenceClass> srcEq, Set<EquivalenceClass> trgEq, SimpleDictionary simpleDict)
  {
    HashMap<String, EquivalenceClass> srcMap = null;
    HashMap<String, EquivalenceClass> trgMap = null;
    
    if (srcEq != null) { 
      srcMap = new HashMap<String, EquivalenceClass>();
    
      for (EquivalenceClass eq : srcEq) {
        for (String sWord : eq.getAllWords()) { 
          assert !srcMap.containsKey(sWord);
          srcMap.put(sWord, eq);
        }
      }
    }

    if (trgEq != null) {
      trgMap = new HashMap<String, EquivalenceClass>();
    
      for (EquivalenceClass eq : trgEq) {
        for (String tWord : eq.getAllWords()) { 
          assert !trgMap.containsKey(tWord);
          trgMap.put(tWord, eq);
        }
      }
    }
  
    HashSet<EquivalenceClass> tEqSet;
    EquivalenceClass sEq;
    EquivalenceClass tEq;
    
    for (String sDictWord : simpleDict.getAllSrc())
    {
      if (srcMap == null) {
        (sEq = new SimpleEquivalenceClass()).init(sDictWord, false);
      } else {
        sEq = srcMap.get(sDictWord); 
      }
           
      if (sEq != null)
      {
        for (String tDictWord : simpleDict.getTrg(sDictWord))
        {
          
          if (trgMap == null) {
            (tEq = new SimpleEquivalenceClass()).init(tDictWord, false);
          } else {
            tEq = trgMap.get(tDictWord);
          }
          
          if (tEq != null)
          {
            if (null == (tEqSet = m_map.get(sEq)))
            { m_map.put(sEq, tEqSet = new HashSet<EquivalenceClass>());
            }
            
            tEqSet.add(tEq);
            
            m_srcMap.put(sEq.getId(), sEq);
            m_trgMap.put(tEq.getId(), tEq);
          }
        }
      }
    }
  }

  public String getName()
  {
    return m_name;
  }

  public Set<EquivalenceClass> getAllSrc()
  {
    return new HashSet<EquivalenceClass>(m_srcMap.values());
  }
  
  public int size()
  {
    return m_map.size();
  }
  
  public Set<EquivalenceClass> getAllTrg()
  {
    return new HashSet<EquivalenceClass>(m_trgMap.values());
  }
  
  public Set<EquivalenceClass> translate(EquivalenceClass srcEq)
  {
    return m_map.get(srcEq);
  }
  
  public boolean containsSrc(EquivalenceClass eq)
  {
    return m_srcMap.containsKey(eq.getId());
  }

  public boolean containsTrg(EquivalenceClass eq)
  {
    return m_trgMap.containsKey(eq.getId());
  }
  
  // Note their could be repeats if we got the same translation through different contextualsrc words
  public List<ContextualItem> translateContext(EquivalenceClass eq)
  {    
    Context oldContext = (Context)eq.getProperty(Context.class.getName());
    LinkedList<ContextualItem> newCis = new LinkedList<ContextualItem>();
    
    if (oldContext != null)
    {
      Collection<ContextualItem> oldCis = oldContext.getContextualItems();
      Long srcEqId;
      HashSet<EquivalenceClass> translations;
      EquivalenceClass srcEq;

      for (ContextualItem oldCi : oldCis)
      {
        srcEqId = oldCi.getContextEqId();
        
        // Look up src eq and then all of its translations
        if ((null != (srcEq = m_srcMap.get(srcEqId))) && (null != (translations = m_map.get(srcEq))))
        {
          // Add a contextual item for each translation keeping the src count
          for (EquivalenceClass trgEq : translations)
          {
            // Context context, Long contEqId, long count
            newCis.add(new ContextualItem(null, trgEq.getId(), oldCi.getCorpusCount(), oldCi.getContextCount()));
          }
        }
      }
    }
    
    return newCis;
  }
  
  public String toString()
  {
    return "Dictionary [" + m_name + "] contains " + m_map.size() + " source language entries.";
  }

  protected HashMap<EquivalenceClass, HashSet<EquivalenceClass>> m_map;
  protected HashMap<Long, EquivalenceClass> m_srcMap;
  protected HashMap<Long, EquivalenceClass> m_trgMap;
  protected String m_name;
}
