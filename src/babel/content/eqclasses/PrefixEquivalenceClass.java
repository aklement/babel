package babel.content.eqclasses;

import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Collection;

/**
 * A word belongs to an equivalence class if it shares a common prefix (starting
 * from the beginning of the word and of size at least MIN_PREFIX_SIZE).
 */
public class PrefixEquivalenceClass extends EquivalenceClass
{
  protected final static int MIN_PREFIX_SIZE = 5;
  
  public PrefixEquivalenceClass()
  {
    super();
    
    m_prefix = null;
    m_allWords = new HashSet<String>();
    m_endings = new HashSet<String>();
  }
  
  public void init(String word, boolean caseSensitive)
  {
    super.init(word, caseSensitive); 
    
    m_prefix = getWordOfAppropriateForm(word, m_caseSensitive);
    m_endings.clear();
    m_endings.add("");
    m_allWords.clear();
  }
  
  public boolean isInEqClass(String word)
  {
    checkInitialized();

    String tmpWord = getWordOfAppropriateForm(word, m_caseSensitive);  
    return (tmpWord != null) && (m_prefix.equals(tmpWord) || (longestCommonPrefix(tmpWord, m_prefix) >= MIN_PREFIX_SIZE));  
  }
  
  public boolean addMorph(String word)
  {
    checkInitialized();
    
    int prefLength = 0;
    String tmpWord = getWordOfAppropriateForm(word, m_caseSensitive);
    boolean added = m_prefix.equals(tmpWord);
    
    if (!added && (tmpWord != null) && ((prefLength = longestCommonPrefix(tmpWord, m_prefix)) >= MIN_PREFIX_SIZE))
    {
      if (prefLength < m_prefix.length())
      {
        processEnding(m_prefix.substring(prefLength, m_prefix.length()));
        m_prefix = m_prefix.substring(0, prefLength);
      }
      
      if (prefLength < tmpWord.length())
      {
        String newEnding = tmpWord.substring(prefLength, tmpWord.length());
        
        if (!m_endings.contains(newEnding))
        { m_endings.add(newEnding);
        }
      }
      else
      { m_endings.add("");
      }

      added = true;
    }
    
    return added;
  }
  
  public String getStem()
  {
    return m_prefix.substring(0, Math.min(MIN_PREFIX_SIZE, m_prefix.length()));
  }
  
  public Collection<String> getAllWords()
  {
    checkInitialized();
    
    if (m_allWords.size() != m_endings.size())
    {
      m_allWords.clear();
      
      for (String ending : m_endings)
      { m_allWords.add(m_prefix + ending);
      }
    }
    
    return m_allWords;
  }
  
  public String persistToString()
  {
    StringBuilder strBld = new StringBuilder();
    
    strBld.append(m_id);
    strBld.append("\t");
    strBld.append(m_initialized);
    strBld.append("\t");
    strBld.append(m_caseSensitive);
    strBld.append("\t");
    strBld.append(toString());
    
    return strBld.toString();    
  }

  public void unpersistFromString(String str) throws Exception
  {
    String[] toks = str.split("\t");
    
    m_id = Long.parseLong(toks[0]);
    m_initialized = Boolean.parseBoolean(toks[1]);
    m_caseSensitive = Boolean.parseBoolean(toks[2]);      
    m_allWords.clear();
    m_endings.clear();
    m_properties.clear();

    StringTokenizer tokenizer = new StringTokenizer(toks[3], "{} ,");
    
    m_prefix = tokenizer.nextToken();
    boolean addedEndings = false;

    // Unpersist endings
    while(tokenizer.hasMoreTokens())
    { 
      m_endings.add(tokenizer.nextToken().substring(1));
      addedEndings = true;
    }
    
    // If single word - add default blank ending
    if (!addedEndings)
    { m_endings.add("");
    }
  }  
  
  public String toString()
  {
    StringBuilder result = new StringBuilder(m_prefix);
    
    if ((m_endings.size() > 1) || 
        ((m_endings.size() == 1) && (!m_endings.contains(""))))
    {
      boolean first = true;
      
      for (String ending : m_endings)
      {
        if (first)
        {
          result.append(" {-");
          first = false;
        }
        else
        { result.append(", -");
        }
        
        result.append(ending);
      }
      
      result.append("}");
    }
    
    return result.toString();
  }
  
  public boolean merge(EquivalenceClass eq)
  {
    boolean merged = false;
    
    if (sameEqClass(eq))
    {
      for (String word : eq.getAllWords())
      { addMorph(word);
      }
    }

    return merged;
  }
  
  protected int longestCommonPrefix(String one, String two)
  {
    int minSize = Math.min(one.length(), two.length());
    int prefLength = 0;
    
    while (prefLength < minSize)
    {
      if (one.charAt(prefLength) == two.charAt(prefLength))
      { prefLength++;
      }
      else
      { break;
      }
    }
    
    return prefLength;
  }
  
  protected void processEnding(String str)
  {
    HashSet<String> newEndings = new HashSet<String>(); 
    
    for (String ending : m_endings)
    { newEndings.add(str + ending);
    }
    
    m_endings = newEndings;
  }
  
  protected String getAllWordsString()
  {
    return toString();
  }
  
  protected String m_prefix;
  protected HashSet<String> m_endings;
  protected HashSet<String> m_allWords;
}
