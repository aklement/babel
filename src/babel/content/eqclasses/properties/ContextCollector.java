package babel.content.eqclasses.properties;

import java.io.BufferedReader;
import java.util.HashMap;

import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;

/**
 * Collects contextual equivalence class.  Does not collect across sentence 
 * boundaries. TODO: does primitive sentence segmentation - re-implement.
 */
public class ContextCollector extends PropertyCollector
{
  public static final Log LOG = LogFactory.getLog(ContextCollector.class);
  
  protected static final String SENT_DELIM_REGEX = "[\\.\\?¿!¡]+";
  protected static final String WORD_DELIM_REGEX = "[\\s\"\'\\-+=,;:«»{}()<>\\[\\]–“’ ]+";
    
  @SuppressWarnings("unchecked")
  public ContextCollector(String eqClassName, boolean caseSensitive, int leftSize, int rightSize, Set<EquivalenceClass> contextEqs) throws Exception
  {
    m_eqClassClass = (Class<? extends EquivalenceClass>)(Class.forName(eqClassName));
    m_caseSensitive = caseSensitive;
    m_leftSize = leftSize;
    m_rightSize = rightSize;
    m_allContextEqsMap = new HashMap<String, EquivalenceClass>(contextEqs.size());
      
    for (EquivalenceClass eq : contextEqs)
    { m_allContextEqsMap.put(eq.getStem(), eq);
    }
  }
  
  // TODO: check all of the binarySearch calls - replace with HashSet?
  public void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> eqs) throws Exception
  {
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    String[] curSents;
    String[] curSentTokens;
    EquivalenceClass tmpEq;
    //EquivalenceClass cntEq;
    //CoOccurrers cntCoOcc;
    Context tmpEqContext;
    int min, max;
      
    // TODO: Not memory efficient - think of something better
    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>(eqs.size());
 
    for (EquivalenceClass eq : eqs)
    { eqsMap.put(eq.getStem(), eq);
    }
      
    while ((curLine = reader.readLine()) != null)
    {
      // Split into likely sentences
      curSents = curLine.split(SENT_DELIM_REGEX);
        
      // Within each sentence, split into words
      for (int numSent = 0; numSent < curSents.length; numSent++ )
      {
        curSentTokens = curSents[numSent].split(WORD_DELIM_REGEX);
   
        for (int numToken = 0; numToken < curSentTokens.length; numToken++)
        { 
          // Put together a temp equiv class for the current token
          tmpEq = (EquivalenceClass)m_eqClassClass.newInstance();
          tmpEq.init(-1, curSentTokens[numToken], m_caseSensitive);

          // Is that a word we care about?
          if (null != (tmpEq = eqsMap.get(tmpEq.getStem())))            
          {        
            // Get/set its context prop
            if ((tmpEqContext = (Context)tmpEq.getProperty(Context.class.getName())) == null)
            { tmpEq.setProperty(tmpEqContext = new Context(tmpEq));
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
                //cntEq = 
                  tmpEqContext.addContextWord(m_eqClassClass, m_caseSensitive, m_allContextEqsMap, curSentTokens[contextIdx]);
                  
               // TODO: Temp
               // if (cntEq != null)
               // {
               //   // Get/set and update the co-occurrers prop
               //   if ((cntCoOcc = (CoOccurrers)cntEq.getProperty(CoOccurrers.class.getName())) == null)
               //   { cntEq.setProperty(cntCoOcc = new CoOccurrers());
               //   }
                    
               //   cntCoOcc.addCoOccurrer(tmpEq);    
               // }
              }
            }
          }
        }
      }
    }

    reader.close();
  }
 
  /** All equivalence classes which from which to construct context. */
  protected HashMap<String, EquivalenceClass> m_allContextEqsMap;
  protected Class<? extends EquivalenceClass> m_eqClassClass;
  protected boolean m_caseSensitive;
  protected int m_leftSize;
  protected int m_rightSize;
}
