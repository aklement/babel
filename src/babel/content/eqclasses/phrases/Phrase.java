package babel.content.eqclasses.phrases;

import java.util.regex.Pattern;

import babel.content.eqclasses.SimpleEquivalenceClass;

public class Phrase extends SimpleEquivalenceClass {
  
  public static final Pattern PHRASE_DELIMS = Pattern.compile("\\s+");

  public void init(String word, boolean caseSensitive)
  {
    super.init(word, caseSensitive);
    
    m_word = getWordOfAppropriateForm(word, m_caseSensitive);
    m_numTokens = PHRASE_DELIMS.split(m_word).length;
  }
  
  public int numTokens() {
    return m_numTokens;
  }
  
  protected int m_numTokens;
}
