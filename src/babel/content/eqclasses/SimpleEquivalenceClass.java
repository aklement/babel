package babel.content.eqclasses;

import java.util.Collection;
import java.util.ArrayList;
import java.util.Collections;

/**
 * Equivalence class includes just the word itself.
 */
public class SimpleEquivalenceClass extends EquivalenceClass
{
  public SimpleEquivalenceClass()
  {
    super();
    
    m_word = null;
    m_allWords = new ArrayList<String>();
  }
  
  public void init(int id, String word, boolean caseSensitive)
  {
    super.init(id, word, caseSensitive);
    
    m_word = getWordOfAppropriateForm(word);
    m_allWords.clear();
  }
  
  public boolean addMorph(String word)
  {
    checkInitialized();
    return m_word.equals(getWordOfAppropriateForm(word)); 
  }
  
  public boolean merge(EquivalenceClass other)
  {
    return overlap(other);
  }
  
  public Collection<String> getAllWords()
  {
    checkInitialized();
    
    if (m_allWords.size() == 0)
    { m_allWords.add(m_word);
    }

    return Collections.unmodifiableList(m_allWords);
  }
  
  public String getStem()
  {
    return m_word;
  }
  
  /**
   * @return simple String representation of EquivalenceClassSimple.
   */
  public String toString()
  {    
    return m_word;
  }
  
  public boolean equals(Object obj)
  {
    return ((obj != null) && (obj instanceof SimpleEquivalenceClass) && 
            (m_caseSensitive == ((SimpleEquivalenceClass)obj).m_caseSensitive) &&
            m_word.equals(((SimpleEquivalenceClass)obj).m_word));
  }
  
  public int compareTo(EquivalenceClass obj) throws ClassCastException
  {
    return m_word.compareTo(((SimpleEquivalenceClass)obj).m_word);
  }
  
  public boolean overlap(EquivalenceClass eqs)
  {
    return equals(eqs);
  }

  @Override
  public String persistToString()
  {
    StringBuilder strBld = new StringBuilder();
    
    strBld.append(m_id);
    strBld.append("\t");
    strBld.append(m_initialized);
    strBld.append("\t");
    strBld.append(m_caseSensitive);
    strBld.append("\t");
    strBld.append(m_word);
    
    return strBld.toString();
  }
  
  @Override
  public void unpersistFromString(String str) throws Exception
  {
    String[] toks = str.split("\t");
      
    m_id = Integer.parseInt(toks[0]);
    m_initialized = Boolean.parseBoolean(toks[1]);
    m_caseSensitive = Boolean.parseBoolean(toks[2]);
    m_word = toks[3];
      
    m_allWords.clear();
    m_properties.clear();
  }
  
  protected String m_word;
  protected ArrayList<String> m_allWords;
}