package babel.content.eqclasses.properties;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.phrases.Phrase;

public class PhraseContext extends Property {
  
  public PhraseContext() {
    m_before = new HashMap<Phrase, Integer>();
    m_after = new HashMap<Phrase, Integer>();
    m_outOfOrder = new HashMap<Phrase, Integer>();
    m_all = new HashSet<Phrase>();
  }

  public void addBefore(Phrase phrase) {
    Integer num = m_before.get(phrase);
    m_before.put(phrase, num == null ? 1 : num + 1);

    m_all.add(phrase);
  }
  
  public void addAfter(Phrase phrase) {
    Integer num = m_after.get(phrase);
    m_after.put(phrase, num == null ? 1 : num + 1);

    m_all.add(phrase);
  }
  
  public void addOutOfOrder(Phrase phrase) {
    Integer num = m_outOfOrder.get(phrase);
    m_outOfOrder.put(phrase, num == null ? 1 : num + 1);
    
    m_all.add(phrase);
  }
  
  public boolean hasAnywhere(Phrase phrase) {
    return m_all.contains(phrase);
  }
  
  public Map<Phrase, Integer> getBefore() {
    return m_before;
  }

  public Map<Phrase, Integer> getAfter() {
    return m_after;
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
  
  protected HashMap<Phrase, Integer> m_before;
  protected HashMap<Phrase, Integer> m_after;
  protected HashMap<Phrase, Integer> m_outOfOrder;
  protected HashSet<Phrase> m_all;
}
