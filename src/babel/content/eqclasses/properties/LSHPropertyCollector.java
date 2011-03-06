package babel.content.eqclasses.properties;

import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;

public abstract class LSHPropertyCollector {
  
  public abstract void collectProperty(Set<? extends EquivalenceClass> eqClasses) throws Exception;  
}
