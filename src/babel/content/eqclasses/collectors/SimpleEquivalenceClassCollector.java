package babel.content.eqclasses.collectors;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.comparators.OverlapComparator;
import babel.content.eqclasses.filters.EquivalenceClassFilter;

public class SimpleEquivalenceClassCollector extends EquivalenceClassCollector
{
  protected static final String WORD_DELIM_REGEX = "[\\s\"\'\\-+=,;:гх{}()<>\\[\\]\\.\\?ю!апрстуй]+";
  protected static final Comparator<EquivalenceClass> OVERLAP_COMPARATOR = new OverlapComparator();

  public SimpleEquivalenceClassCollector(String eqClassName, List<EquivalenceClassFilter> filters, boolean caseSensitive)
  {
    super(eqClassName, filters, caseSensitive);
  }
  
  public SimpleEquivalenceClassCollector(String eqClassName, boolean caseSensitive)
  {
    this(eqClassName, null, caseSensitive);
  }
  
  @Override
  public Set<EquivalenceClass> collect(InputStreamReader corpusReader, int maxEquivalenceClass) throws Exception
  {
    BufferedReader reader = new BufferedReader(corpusReader);
    HashMap<String, EquivalenceClass> eqs = new HashMap<String, EquivalenceClass>();
    String line;
    String[] toks;
    EquivalenceClass tmpeq, eq;
    int count = 0;

    while ((line = reader.readLine()) != null)
    {
      toks = line.split(WORD_DELIM_REGEX);
        
      for (String tok : toks)
      {
        tmpeq = (EquivalenceClass)m_eqClass.newInstance();
        tmpeq.init(CURRENT_EQCLASS_ID, tok, m_eqCaseSensitive);
                 
        if (null != (eq = eqs.get(tmpeq.getStem())))
        {
          eq.merge(tmpeq);
        }
        else if ((maxEquivalenceClass < 0 || count < maxEquivalenceClass) && keep(tmpeq, m_filters))
        {
          eqs.put(tmpeq.getStem(), tmpeq);
          CURRENT_EQCLASS_ID++;
          count++;
        }
      }
    }
 
    return new HashSet<EquivalenceClass>(eqs.values());
  }
}
