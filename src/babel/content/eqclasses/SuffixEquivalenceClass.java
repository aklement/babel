package babel.content.eqclasses;

import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.Collection;

/**
 * A word belongs to an equivalence class if it shares a common SUFFIX (starting
 * from the end of the word and of size at least MIN_PREFIX_SIZE).
 */
public class SuffixEquivalenceClass extends EquivalenceClass
{
  protected final static int MIN_SUFFIX_SIZE = 5;
  
  public SuffixEquivalenceClass()
  {
    super();
    
    m_suffix = null;
    m_allWords = new HashSet<String>();
    m_beginnings = new HashSet<String>();
  }
  
  public void init(String word, boolean caseSensitive)
  {
    super.init(word, caseSensitive); 
    
    m_suffix = getWordOfAppropriateForm(word, m_caseSensitive);
    m_beginnings.clear();
    m_beginnings.add("");
    m_allWords.clear();
  }
  
  public boolean isInEqClass(String word)
  {
    checkInitialized();

    String tmpWord = getWordOfAppropriateForm(word, m_caseSensitive);  
    return (tmpWord != null) && (m_suffix.equals(tmpWord) || (longestCommonSuffix(tmpWord, m_suffix) >= MIN_SUFFIX_SIZE));  
  }
  
  public boolean addMorph(String word)
  {
    checkInitialized();
    
    int sufLength = 0;
    String tmpWord = getWordOfAppropriateForm(word, m_caseSensitive);
    boolean added = m_suffix.equals(tmpWord);
    
    if (!added && (tmpWord != null) && ((sufLength = longestCommonSuffix(tmpWord, m_suffix)) >= MIN_SUFFIX_SIZE))
    {
      if (sufLength < m_suffix.length())
      {
        processBeginning(m_suffix.substring(0, m_suffix.length()-sufLength));
        m_suffix = m_suffix.substring(m_suffix.length()-sufLength, m_suffix.length());
      }
      
      if (sufLength < tmpWord.length())
      {
        String newBeginning = tmpWord.substring(0, tmpWord.length()-sufLength);
        
        if (!m_beginnings.contains(newBeginning))
        { m_beginnings.add(newBeginning);
        }
      }
      else
      { m_beginnings.add("");
      }

      added = true;
    }
    
    return added;
  }
  
  public String getStem()
  {
	return m_suffix.substring(Math.max(m_suffix.length()-MIN_SUFFIX_SIZE,0),m_suffix.length());
  }
  
  public Collection<String> getAllWords()
  {
    checkInitialized();
    
    if (m_allWords.size() != m_beginnings.size())
    {
      m_allWords.clear();
      
      for (String beginning : m_beginnings)
      { m_allWords.add(beginning + m_suffix);
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
    m_beginnings.clear();
    m_properties.clear();

    StringTokenizer tokenizer = new StringTokenizer(toks[3], "{} ,");
    
    m_suffix = tokenizer.nextToken();
    boolean addedBeginnings = false;

    // Unpersist endings
    while(tokenizer.hasMoreTokens())
    { 
      m_beginnings.add(tokenizer.nextToken().substring(1));
      addedBeginnings = true;
    }
    
    // If single word - add default blank ending
    if (!addedBeginnings)
    { m_beginnings.add("");
    }
  }  
  
  public String toString()
  {
    StringBuilder result = new StringBuilder("");
    
    if ((m_beginnings.size() > 1) || 
        ((m_beginnings.size() == 1) && (!m_beginnings.contains(""))))
    {
      boolean first = true;
      
      for (String beginning : m_beginnings)
      {
        if (first)
        {
          result.append(" {-");
          first = false;
        }
        else
        { result.append(", -");
        }
        
        result.append(beginning);
      }
      
      result.append("}");
    }
    result.append(m_suffix);
    
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
  
  protected int longestCommonSuffix(String one, String two)
  {
    int minSize = Math.min(one.length(), two.length());
    int sufLength = minSize;
    int endPosition=0;
    
    while (sufLength < minSize)
    {
      endPosition++;
      if (one.charAt(one.length()-endPosition) == two.charAt(two.length()-endPosition))
      { sufLength++;
      }
      else
      { break;
      }
    }
    
    return sufLength;
  }
  
  protected void processBeginning(String str)
  {
    HashSet<String> newBeginnings = new HashSet<String>(); 
    
    for (String beginning : m_beginnings)
    { newBeginnings.add(beginning + str);
    }
    
    m_beginnings = newBeginnings;
  }
  
  protected String getAllWordsString()
  {
    return toString();
  }
  
  protected String m_suffix;
  protected HashSet<String> m_beginnings;
  protected HashSet<String> m_allWords;
}
