package babel.content.eqclasses.properties;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Type.EqType;

public class NumberAndTypeCollector extends PropertyCollector
{
  public static final Log LOG = LogFactory.getLog(NumberAndTypeCollector.class);

  protected static final String DELIM_REGEX = "[\\s\"\'\\-+=,;:«»{}()<>\\[\\]\\.\\?¿!¡–“’ ]+";
    
  @SuppressWarnings("unchecked")
  public NumberAndTypeCollector(String eqClassName, boolean caseSensitive, EqType type) throws Exception
  {
    m_eqClassClass = (Class<? extends EquivalenceClass>)(Class.forName(eqClassName));
    m_caseSensitive = caseSensitive;
    m_eqType = type;
  }
  
  public void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> eqs) throws Exception
  {
    BufferedReader reader = new BufferedReader(corpusAccess.getCorpusReader());
    String curLine;
    String[] curTokens;
    EquivalenceClass tmpEq;
    Number tmpEqNumber;
      
    // TODO: Not memory efficient - think of something better
    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>(eqs.size());
 
    for (EquivalenceClass eq : eqs)
    { eqsMap.put(eq.getStem(), eq);
    }
      
    while ((curLine = reader.readLine()) != null)
    {
      curTokens = curLine.split(DELIM_REGEX);
        
      for (int numToken = 0; numToken < curTokens.length; numToken++)
      { 
        // Put together a temp equiv class for the current token
        tmpEq = (EquivalenceClass)m_eqClassClass.newInstance();
        tmpEq.init(-1, curTokens[numToken], m_caseSensitive);

        // Is that a word we care about?
        if (null != (tmpEq = eqsMap.get(tmpEq.getStem())))            
        {
          // Set its type property
          if ((Type)tmpEq.getProperty(Type.class.getName()) == null)
          { tmpEq.setProperty(new Type(m_eqType));
          }

          // Get/set and increment its number prop
          if ((tmpEqNumber = (Number)tmpEq.getProperty(Number.class.getName())) == null)
          { tmpEq.setProperty(tmpEqNumber = new Number());
          }

          tmpEqNumber.increment();
        }
      }
    }

    reader.close();
  }
 
  /** All equivalence classes which from which to construct context. */
  protected Class<? extends EquivalenceClass> m_eqClassClass;
  protected boolean m_caseSensitive;
  protected EqType m_eqType;
}