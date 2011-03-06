package babel.content.eqclasses.properties.order;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.comparators.NumberComparator;
import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.properties.Property;

public class PhraseContext extends Property {
  
  public PhraseContext() {
    m_before = new HashMap<Phrase, Integer>();
    m_after = new HashMap<Phrase, Integer>();
    m_outOfOrder = new HashMap<Phrase, Integer>();
    m_all = new HashSet<Phrase>();
    m_keepProb = 1.0;
    m_rand = null;
  }

  public PhraseContext(double keepProb) {
    this();
    m_keepProb = keepProb;
    m_rand = new Random(1L);
  }
  
  public void addBefore(Phrase phrase) {
    
    if ((m_keepProb == 1.0) || keep()) {
      Integer num = m_before.get(phrase);
      m_before.put(phrase, num == null ? 1 : num + 1);

      m_all.add(phrase);
    }
  }
  
  public void addAfter(Phrase phrase) {
    
    if ((m_keepProb == 1.0) || keep()) {
      Integer num = m_after.get(phrase);
      m_after.put(phrase, num == null ? 1 : num + 1);

      m_all.add(phrase);
    }
  }
  
  public void addOutOfOrder(Phrase phrase) {
    
    if ((m_keepProb == 1.0) || keep()) {
      Integer num = m_outOfOrder.get(phrase);
      m_outOfOrder.put(phrase, num == null ? 1 : num + 1);
    
      m_all.add(phrase);
    }
  }
  
  public boolean hasAnywhere(Phrase phrase) {
    return m_all.contains(phrase);
  }
  
  public Set<Phrase> getAll() {
    return m_all;
  }
  
  public Map<Phrase, Integer> getBefore() {
    return m_before;
  }

  public Map<Phrase, Integer> getAfter() {
    return m_after;
  }

  public Map<Phrase, Integer> getDiscontinuous() {
    return m_outOfOrder;
  }
  
  public int beforeCount(Phrase phrase) {
    return m_before.containsKey(phrase) ? m_before.get(phrase) : 0;
  }
  
  public int afterCount(Phrase phrase) {
    return m_after.containsKey(phrase) ? m_after.get(phrase) : 0;
  }
  
  public int outOfOrderCount(Phrase phrase) {
    return m_outOfOrder.containsKey(phrase) ? m_outOfOrder.get(phrase) : 0;
  }
  
  public String persistToString() {
    assert false : "Not implemented";
    return null;
  }

  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception {
    assert false : "Not implemented";
  }
  
  public void pruneMostFreq(int keepBefore, int keepAfter, int keepDisc) {
    
    LinkedList<Phrase> phraseList;
    
    if ((keepBefore >= 0) && (keepBefore < m_before.size())) {
      phraseList = new LinkedList<Phrase>(m_before.keySet());
      Collections.sort(phraseList, new NumberComparator(true));
      
      for (int i = keepBefore; i < phraseList.size(); i++) {
        m_before.remove(phraseList.get(i));
      }
    }

    if ((keepAfter >= 0) && (keepAfter < m_after.size())) {
      phraseList = new LinkedList<Phrase>(m_after.keySet());
      Collections.sort(phraseList, new NumberComparator(true));
      
      for (int i = keepAfter; i < phraseList.size(); i++) {
        m_after.remove(phraseList.get(i));
      }
    }

    if ((keepDisc >= 0) && (keepDisc < m_outOfOrder.size())) {
      phraseList = new LinkedList<Phrase>(m_outOfOrder.keySet());
      Collections.sort(phraseList, new NumberComparator(true));
      
      for (int i = keepDisc; i < phraseList.size(); i++) {
        m_outOfOrder.remove(phraseList.get(i));
      }
    }
    
    m_all.clear();
    m_all.addAll(m_before.keySet());
    m_all.addAll(m_after.keySet());
    m_all.addAll(m_outOfOrder.keySet());
  }
  
  protected synchronized boolean keep() {
    return (m_rand.nextDouble() < m_keepProb);
  }
  
  protected Random m_rand;
  protected double m_keepProb;
  protected HashMap<Phrase, Integer> m_before;
  protected HashMap<Phrase, Integer> m_after;
  protected HashMap<Phrase, Integer> m_outOfOrder;
  protected HashSet<Phrase> m_all;
}
