package babel.content.eqclasses.properties.context;

import java.io.BufferedReader;
import java.util.HashMap;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.PropertyCollector;

/**
 * Collects contextual equivalence class.  Does not collect across sentence 
 * boundaries. TODO: does primitive sentence segmentation - re-implement.
 */
public class ContextCollector extends PropertyCollector
{
  public static final Log LOG = LogFactory.getLog(ContextCollector.class);

  public ContextCollector(boolean caseSensitive, boolean posSensitive, int leftSize, int rightSize, Set<EquivalenceClass> contextEqs) throws Exception
  {
	  super(caseSensitive);
    
    m_leftSize = leftSize;
    m_rightSize = rightSize;
    //Map from context eq words to eq object itself. Context eqs here already have window position attached is pos sensitive
    m_allContextEqsMap = new HashMap<String, EquivalenceClass>(contextEqs.size());
    m_positionSensitive = posSensitive;
    
    for (EquivalenceClass eq : contextEqs)
    {
      for (String word : eq.getAllWords())
      { 
    		assert m_allContextEqsMap.get(word) == null;
    		m_allContextEqsMap.put(word, eq);
      }
    }
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, Set<? extends EquivalenceClass> eqs) throws Exception
  {
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    String[] curSents;
    String[] curSentTokens;
    EquivalenceClass foundEq;
    Context fountEqContext;
    int min, max;
      
    // TODO: Very inefficient - think of something better
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
      // Split into likely sentences
      curSents = sentSplit(curLine, corpusAccess.isOneSentencePerLine());
        
      // Within each sentence, split into words
      for (int numSent = 0; numSent < curSents.length; numSent++ )
      {
        curSentTokens = curSents[numSent].split(WORD_DELIM_REGEX);
   
        for (int numToken = 0; numToken < curSentTokens.length; numToken++)
        {         
          // Look for the word's equivalence class
          if (null != (foundEq = eqsMap.get(EquivalenceClass.getWordOfAppropriateForm(curSentTokens[numToken], m_caseSensitive))))               
          {        
            // Get/set its context prop
            if ((fountEqContext = (Context)foundEq.getProperty(Context.class.getName())) == null)
            { foundEq.setProperty(fountEqContext = new Context(foundEq));
            }
   
            // A window around the word
            min = Math.max(0, numToken - m_leftSize);
            max = Math.min(numToken + m_rightSize + 1, curSentTokens.length);
            
            // Add all words in the contextual window (except for the word itself).
            for (int contextIdx = min; contextIdx < max; contextIdx++)
            {
              if (contextIdx != numToken)
              { 
                // Add current word to the current equivalence class context
            	if (!m_positionSensitive){
            		fountEqContext.addContextWord(m_caseSensitive, m_allContextEqsMap, curSentTokens[contextIdx]);
            	}
            	else{
                    int distAway=contextIdx-numToken;
                    String contextword = curSentTokens[contextIdx]+"|s:"+distAway;
                    //LOG.info("Adding "+contextword+" as context for: "+curSentTokens[numToken]);
                    fountEqContext.addContextWord(m_caseSensitive, m_allContextEqsMap, contextword);
            	}
              }
            }
          }
        }
      }
    }

    reader.close();
  }
  
  protected String[] sentSplit(String line, boolean oneSentPerLine)
  {
    String[] sents;
    
    if (oneSentPerLine)
    { 
      sents = new String[1];
      sents[0] = line;
    }
    else
    { 
      sents = line.split(SENT_DELIM_REGEX);
    }
    
    return sents;
  }
 
  /** All equivalence classes which from which to construct context. */
  protected HashMap<String, EquivalenceClass> m_allContextEqsMap;
  protected int m_leftSize;
  protected int m_rightSize;
  protected boolean m_positionSensitive;
}
