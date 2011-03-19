package babel.content.eqclasses.phrases;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class PhraseSet {

  protected static final String DEFAULT_CHARSET = "UTF-8";

  public PhraseSet (String phrasesFile, boolean caseSensitive, int maxPhraseLength) throws Exception {
    this(phrasesFile, DEFAULT_CHARSET, caseSensitive, maxPhraseLength);
  }
  
  public PhraseSet (String phrasesFile, String encoding, boolean caseSensitive, int maxPhraseLength) throws Exception {
    m_phrases = new HashSet<Phrase>();
    processPhraseListFile(phrasesFile, encoding, caseSensitive, maxPhraseLength);
  }
  
  public Set<Phrase> getPhrases() {
    return m_phrases;
  }
  
  protected void processPhraseListFile(String phrasesFile, String encoding, boolean caseSensitive, int maxPhraseLength) throws IOException {
       
    InputStream is = new FileInputStream(phrasesFile);
      
    if (phrasesFile.toLowerCase().endsWith("gz"))
    { is = new GZIPInputStream(is);
    }
      
    BufferedReader fileReader = new BufferedReader(new InputStreamReader(is, encoding));
    String line = null;
    Phrase phrase;
    
    while ((line = fileReader.readLine()) != null) {
    
      (phrase = new Phrase()).init(line, caseSensitive);
        
      if (((maxPhraseLength < 0) || (phrase.numTokens() <= maxPhraseLength)) && !m_phrases.contains(phrase)) {
        phrase.assignId();
        m_phrases.add(phrase);          
      }
    }
    
    fileReader.close();
  }
    
  protected Set<Phrase> m_phrases;
}
