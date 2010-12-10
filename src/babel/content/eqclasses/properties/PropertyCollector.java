package babel.content.eqclasses.properties;

import java.util.Set;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;

/**
 * Collects property values.
 */
public abstract class PropertyCollector {
  
  public static final String SENT_DELIM_REGEX = "[\\|\\.\\?¿!¡]+";    
  public static final String WORD_DELIM_REGEX = "[\\|\\$\\*\\s\"\'\\-\\+=,;:«»{}()<>\\[\\]\\.\\?¿!¡–“”‘’ ]+";

  protected PropertyCollector(boolean caseSensitive) {
    m_caseSensitive = caseSensitive;
  }
  
  /**
   * @param corpusAccess Corpus from which to collect the property.
   * eqClasses equivalence class for which to gather properties.
   */
  public abstract void collectProperty(CorpusAccessor corpusAccess, Set<? extends EquivalenceClass> eqClasses) throws Exception;
  
  protected boolean m_caseSensitive;
}
