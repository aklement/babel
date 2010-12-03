package babel.content.eqclasses.collectors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.content.eqclasses.filters.EquivalenceClassFilter;
import babel.content.eqclasses.properties.PropertyCollector;

public class SimpleEquivalenceClassCollector extends EquivalenceClassCollector
{
  protected static final String WORD_DELIM_REGEX = PropertyCollector.WORD_DELIM_REGEX;

  public SimpleEquivalenceClassCollector(List<EquivalenceClassFilter> filters, boolean caseSensitive)
  {
    super(SimpleEquivalenceClass.class.getName(), filters, caseSensitive);
  }
  
  public SimpleEquivalenceClassCollector(boolean caseSensitive)
  {
    this(null, caseSensitive);
  }
  
  @Override
  public Set<EquivalenceClass> collect(InputStreamReader corpusReader, int maxEquivalenceClass) throws Exception
  {
    BufferedReader reader = new BufferedReader(corpusReader);
    HashMap<String, SimpleEquivalenceClass> eqs = new HashMap<String, SimpleEquivalenceClass>();
    String line;
    String[] toks;
    SimpleEquivalenceClass tmpEq;
    int count = 0;    
    
    while ((line = reader.readLine()) != null)
    {
      toks = line.split(WORD_DELIM_REGEX);
        
      for (String tok : toks)
      {
        tmpEq = (SimpleEquivalenceClass)m_eqClass.newInstance();
        tmpEq.init(tok, m_eqCaseSensitive);
        
        if (!eqs.containsKey(tmpEq.getWord()) && (maxEquivalenceClass < 0 || count < maxEquivalenceClass) && keep(tmpEq, m_filters))
        {
          tmpEq.assignId();
          eqs.put(tmpEq.getWord(), tmpEq);
          count++;
        }
      }
    }    
    
    reader.close();
    
    return new HashSet<EquivalenceClass>(eqs.values());
  }
}
