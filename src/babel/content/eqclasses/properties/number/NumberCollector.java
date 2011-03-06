package babel.content.eqclasses.properties.number;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.PropertyCollector;

public class NumberCollector extends PropertyCollector
{
  public static final Log LOG = LogFactory.getLog(NumberCollector.class);
  
  public NumberCollector(boolean caseSensitive) throws Exception
  {
    super(caseSensitive);
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, Set<? extends EquivalenceClass> eqs) throws Exception
  {
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    String[] curTokens;
    EquivalenceClass foundEq;
    Number tmpEqNumber;
      
    // TODO: Inefficient - think of something better
    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>(eqs.size());
 
    for (EquivalenceClass eq : eqs)
    { 
      for (String word : eq.getAllWords())
      { 
        assert eqsMap.get(word) == null;
        eqsMap.put(word, eq);
      }
    }
      
    while ((curLine = reader.readLine()) != null)
    {
      curTokens = curLine.split(WORD_DELIM_REGEX);
        
      for (int numToken = 0; numToken < curTokens.length; numToken++)
      {
        // Look for the word's equivalence class
        if (null != (foundEq = eqsMap.get(EquivalenceClass.getWordOfAppropriateForm(curTokens[numToken], m_caseSensitive))))            
        {
          // Get/set and increment its number prop
          if ((tmpEqNumber = (Number)foundEq.getProperty(Number.class.getName())) == null)
          { foundEq.setProperty(tmpEqNumber = new Number());
          }

          tmpEqNumber.increment();
        }
      }
    }

    reader.close();
  } 
}