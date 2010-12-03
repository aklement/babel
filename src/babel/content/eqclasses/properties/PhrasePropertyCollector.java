package babel.content.eqclasses.properties;

import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import babel.content.eqclasses.phrases.Phrase;
import babel.util.misc.InvertibleHashMap;

public abstract class PhrasePropertyCollector extends PropertyCollector {
  
  protected static final Pattern SENT_DELIMS = Pattern.compile("[\\.\\?¿!¡]+");
  protected static final Pattern PHRASE_DELIMS = Phrase.PHRASE_DELIMS;
  protected static final Pattern WORD_DELIMS = Pattern.compile(WORD_DELIM_REGEX);  

  protected PhrasePropertyCollector(int maxPhraseLength, boolean caseSensitive) {
    super(caseSensitive);
    
    m_maxPhraseLength = maxPhraseLength;
  }

  protected static List<String> getSentences(String str, boolean oneSentPerLine) {
    
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
  
  // Returns a map of delimiter index pairs mapped to the ordinal number of the delimiter in the sentence
  protected static InvertibleHashMap<IdxPair, Integer> getAllDelims(String sent, Pattern delimPattern) {
    
    InvertibleHashMap<IdxPair, Integer> spaces = new InvertibleHashMap<IdxPair, Integer>();    
    Matcher m = delimPattern.matcher(sent);
    boolean startsWithDelim = false;
    boolean endsWithDelim = false;
    int idx = 1;
    int sentLength = sent.length();

    if (sent.length() > 0) {

      while (m.find()) {
      
        if (m.start() == 0) {
          startsWithDelim = true;
          idx = 0;
        }
      
        if (m.end() == sentLength) {
          endsWithDelim = true;
        }
    
        spaces.put(new IdxPair(m.start(), m.end()), idx++);
      }
    
      if (!startsWithDelim) {
        spaces.put(new IdxPair(0, 0), 0);
      }
      
      if (!endsWithDelim) {
        spaces.put(new IdxPair(sent.length(), sent.length()), idx);
      }
    }

    return spaces;
  }

  protected static List<IdxPair> getAllPhraseIdxs(InvertibleHashMap<IdxPair, Integer> delims, int maxPhraseLength) {
    
    List<IdxPair> phrases = new LinkedList<IdxPair>();
         
    for (int i = 0; i < delims.size() - 1; i++) {
      for (int j = i+1; (maxPhraseLength < 0 || maxPhraseLength >= j-i) && j < delims.size(); j++) {              
        phrases.add(new IdxPair(delims.getKey(i).to, delims.getKey(j).from));
      }
    }
    
    return phrases;
  }

  protected static List<IdxPair> getAllWordIdxs(String sent) {
    
    return getAllPhraseIdxs(getAllDelims(sent, WORD_DELIMS), 1);
  }
  
  protected static List<IdxPair> getAllPhraseIdxs(String sent, int maxPhraseLength) {
    
    return getAllPhraseIdxs(getAllDelims(sent, PHRASE_DELIMS), maxPhraseLength);
  }
  
  protected int m_maxPhraseLength;
  
  public static class IdxPair {
 
    public IdxPair(int from, int to) {
      this.from = from;
      this.to = to;
    }
    
    @Override
    public int hashCode() {
      return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return (obj instanceof IdxPair) && (((IdxPair)obj).from == from) && (((IdxPair)obj).to == to);
    }

    public String toString() {
      return strRep == null ? strRep = (from + "->" + to) : strRep;
    }
    
    public final int from, to;
    private String strRep = null;
  }
}
