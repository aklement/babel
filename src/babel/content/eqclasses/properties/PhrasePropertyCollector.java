package babel.content.eqclasses.properties;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class PhrasePropertyCollector extends PropertyCollector {
  
  protected static final Pattern SENT_DELIMS = Pattern.compile("[\\.\\?¿!¡]+");
  protected static final Pattern PHRASE_DELIMS = Pattern.compile("\\s+");
  
  protected PhrasePropertyCollector(int maxPhraseLength) {
    m_maxPhraseLength = maxPhraseLength;
  }

  public static List<String> getSentences(String str, boolean oneSentPerLine) {
    
    List<String> sents = new LinkedList<String>();
    
    if (oneSentPerLine) {
      sents.add(str.substring(0, str.length()));
      
    } else {
      int sFrom = 0;
      Matcher m = SENT_DELIMS.matcher(str);
      
      while (m.find()) {
        sents.add(str.substring(sFrom, m.end()));
        sFrom = m.end();
      }
      
      if (sFrom != str.length()) {
        sents.add(str.substring(sFrom, str.length()));
      }
    }
    
    return sents;
  }
  
  public static List<IdxPair> getAllPhraseIdxs(String sent, int maxPhraseLength) {
    
    List<IdxPair> spaces = new LinkedList<IdxPair>();
    Matcher m = PHRASE_DELIMS.matcher(sent);
    
    while (m.find()) {
      spaces.add(new IdxPair(m.start(), m.end()));
    }
    
    if (sent.length() > 0) {
      spaces.add(0, new IdxPair(0, 0));
      spaces.add(new IdxPair(sent.length(), sent.length()));
    }
    
    List<IdxPair> phrases = new LinkedList<IdxPair>();
         
    for (int i = 0; i < spaces.size() - 1; i++) {
      for (int j = i+1; (maxPhraseLength < 0 || maxPhraseLength >= j-i) && j < spaces.size(); j++) {              
        phrases.add(new IdxPair(spaces.get(i).to, spaces.get(j).from));
      }
    }
    
    return phrases;
  }
  
  protected int m_maxPhraseLength;
  
  public static class IdxPair {
    public IdxPair(int from, int to) {
      this.from = from;
      this.to = to;
    }
    
    public String toString() {
      return from + "->" + to;
    }
    
    public int from, to;
  }
}
