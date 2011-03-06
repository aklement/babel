package babel.content.eqclasses.properties.lshtime;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.LSHProperty;

public class LSHTimeDistribution extends LSHProperty {
  
  public LSHTimeDistribution() {
    super(null, null);
  }
 
  public LSHTimeDistribution(EquivalenceClass eq, byte[] signature) {
    super(eq, signature);
  }
}