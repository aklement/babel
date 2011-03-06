package babel.content.eqclasses.properties.lshcontext;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.LSHProperty;

public class LSHContext extends LSHProperty {
  
  public LSHContext() {
    super(null, null);
  }
 
  public LSHContext(EquivalenceClass eq, byte[] signature) {
    super(eq, signature);
  }
}
