package babel.content.eqclasses.properties;

import java.util.Set;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;

/**
 * Collects property values.
 */
public abstract class PropertyCollector
{  
  /**
   * @param corpusAccess Corpus from which to collect the property.
   * eqClasses equivalence class for which to gather properties.
   */
  public abstract void collectProperty(CorpusAccessor corpusAccess, Set<EquivalenceClass> eqClasses) throws Exception;
}
