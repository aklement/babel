package babel.content.eqclasses.properties;

import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;

public abstract class LSHPropertyCollector {
  
  protected LSHPropertyCollector(boolean removeOrigProp) {
    m_removeOrigProp = removeOrigProp;
  }
  
  public abstract void collectProperty(Set<? extends EquivalenceClass> eqClasses) throws Exception;
  
  protected boolean m_removeOrigProp;
}
