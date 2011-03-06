package babel.content.eqclasses.properties.number;

import java.io.BufferedReader;
import java.util.List;
import java.util.Set;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.properties.PhrasePropertyCollector;
import babel.util.misc.GettableHashSet;

public class PhraseNumberCollector extends PhrasePropertyCollector {

  public PhraseNumberCollector(int maxPhraseLength, boolean caseSensitive) {
    super(maxPhraseLength, caseSensitive);
    
  }

  public void collectProperty(CorpusAccessor corpusAccess, Set<? extends EquivalenceClass> phrases) throws Exception {

    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    List<String> curSents;
    List<IdxPair> sentPhraseIdxs; 
    GettableHashSet<EquivalenceClass> phMap = new GettableHashSet<EquivalenceClass>(phrases);
    Phrase tmpPhrase, foundPhrase;
    Number foundNumber;
 
    while ((curLine = reader.readLine()) != null) {
      curLine = curLine.trim();
    
      // Split into likely sentences
      curSents = getSentences(curLine, corpusAccess.isOneSentencePerLine());
      
      // Within each sentence, look for phrases
      for (String sent : curSents) {
 
        // Get all phrases up to length m_maxPhraseLength
        sentPhraseIdxs = getAllPhraseIdxs(sent, m_maxPhraseLength);
        
        for (IdxPair phraseIdx : sentPhraseIdxs) {
          
          (tmpPhrase = new Phrase()).init(sent.substring(phraseIdx.from, phraseIdx.to), m_caseSensitive);
          
          if (null != (foundPhrase = (Phrase)phMap.get(tmpPhrase))) {
            
            // Get/set and increment its number prop
            if ((foundNumber = (Number)foundPhrase.getProperty(Number.class.getName())) == null)
            { foundPhrase.setProperty(foundNumber = new Number());
            }

            foundNumber.increment();
          }
        }
      }
    }

    reader.close();    
  }
}
