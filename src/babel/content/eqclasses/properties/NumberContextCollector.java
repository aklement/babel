package babel.content.eqclasses.properties;

import java.io.BufferedReader;
import java.util.List;
import java.util.Collections;
import java.util.Comparator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.comparators.OverlapComparator;

/**
 * Collects contextual equivalence class.  Does not collect across sentence 
 * boundaries. TODO: does primitive sentence segmentation - re-implement.
 */
public class NumberContextCollector extends PropCollector
{
  public static final Log LOG = LogFactory.getLog(NumberContextCollector.class);
  
  protected static final String SENT_DELIM_REGEX = "[.?!]+";
  protected static final String WORD_DELIM_REGEX = "[\\s\"\'\\-+=,;:ÇÈ{}()<>\\[\\]]+";
  
  protected static final Comparator<EquivalenceClass> OVERLAP_COMPARATOR = new OverlapComparator();
  
  public NumberContextCollector(String eqClassName, boolean caseSensitive, int leftSize, int rightSize, List<EquivalenceClass> contextEq)
  {
    try
    {
      m_eqClassClass = (Class<? extends EquivalenceClass>)(Class.forName(eqClassName));
      m_caseSensitive = caseSensitive;
      m_leftSize = leftSize;
      m_rightSize = rightSize;
      m_allContextEq = contextEq;
    }
    catch(Exception e)
    {
      throw new IllegalArgumentException(e.toString());
    }
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, List<EquivalenceClass> eqClasses)
  {
    try
    {
      BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
      String curLine;
      String[] curSents;
      String[] curSentTokens;
      EquivalenceClass tmpEq;
      Context tmpEqContext;
      Number tmpEqNumber;
      int tmpIndex;
      int min, max;

      // Sort the equivalence class first, so that we can search quicker
      Collections.sort(eqClasses, OVERLAP_COMPARATOR);
      
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
            
            // Find it in the list
            if ((tmpIndex = Collections.binarySearch(m_allContextEq, tmpEq, OVERLAP_COMPARATOR)) >= 0)
            {
              // Get the real equiv class from the collection
              tmpEq = (EquivalenceClass)eqClasses.get(tmpIndex);
              
              // Get/set and increment its number prop
              if ((tmpEqNumber = (Number)tmpEq.getProperty(Number.class.getName())) == null)
              { tmpEq.setProperty(tmpEqNumber = new Number());
              }
              
              tmpEqNumber.increment();
              
              // Get/set its context prop
              if ((tmpEqContext = (Context)tmpEq.getProperty(Context.class.getName())) == null)
              { tmpEq.setProperty(tmpEqContext = new Context());
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
                  tmpEqContext.addContextWord(m_eqClassClass, m_caseSensitive, m_allContextEq, curSentTokens[contextIdx]);
                }
              }
            }
          }
        }
      }

      reader.close();
    } 
    catch (Exception e)
    {
      if (LOG.isErrorEnabled())
      {
        LOG.error("Error reading from input stream: " + e.toString());
      }
    }
  }
 
  /** All equivalence classes which from which to construct context. */
  protected List<EquivalenceClass> m_allContextEq;
  protected Class<? extends EquivalenceClass> m_eqClassClass;
  protected boolean m_caseSensitive;
  protected int m_leftSize;
  protected int m_rightSize;
}
