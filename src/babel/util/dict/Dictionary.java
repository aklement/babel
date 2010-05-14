package babel.util.dict;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Context;
import babel.content.eqclasses.properties.Context.ContextualItem;

public class Dictionary
{
  public Dictionary(Set<EquivalenceClass> srcEqs, Set<EquivalenceClass> trgEqs, SimpleDictionary simpleDict, String name)
  {
    m_rand = new Random(1);
    m_map = new HashMap<EquivalenceClass, HashSet<EquivalenceClass>>();
    m_name = name;
    m_srcMap = new HashMap<Long, EquivalenceClass>();
    m_trgMap = new HashMap<Long, EquivalenceClass>(); 
    
    costruct(srcEqs, trgEqs, simpleDict);
  }
  
  protected void costruct(Set<EquivalenceClass> srcEq, Set<EquivalenceClass> trgEq, SimpleDictionary simpleDict)
  {
    HashMap<String, EquivalenceClass> srcMap = new HashMap<String, EquivalenceClass>();
    HashMap<String, EquivalenceClass> trgMap = new HashMap<String, EquivalenceClass>();

    for (EquivalenceClass EquivalenceClass : srcEq)
    {
      for (String sWord : EquivalenceClass.getAllWords())
      { 
        assert !srcMap.containsKey(sWord);
        srcMap.put(sWord, EquivalenceClass);
      }
    }

    for (EquivalenceClass EquivalenceClass : trgEq)
    {
      for (String tWord : EquivalenceClass.getAllWords())
      { 
        assert !trgMap.containsKey(tWord);
        trgMap.put(tWord, EquivalenceClass);
      }
    }
  
    HashSet<EquivalenceClass> tEqSet;
    EquivalenceClass sEq;
    EquivalenceClass tEq;
    
    for (String sDictWord : simpleDict.getAllSrc())
    {
      if (null != (sEq = srcMap.get(sDictWord)))
      {
        for (String tDictWord : simpleDict.getTrg(sDictWord))
        {
          if (null != (tEq = trgMap.get(tDictWord)))
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
  protected Random m_rand;
  protected String m_name;
}
