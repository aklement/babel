package babel.content.eqclasses.properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;

/**
 * Superclass for property of an EquivalnceClass.
 */
public abstract class Property
{
  protected static final Log LOG = LogFactory.getLog(Property.class);

  /**
   * @return A property ID String (a class name extending this class).
   */
  public String getPropertyId()
  { return getClass().getName();
  }
  
  public abstract String persistToString() throws Exception;
  public abstract void unpersistFromString(EquivalenceClass eq, String str) throws Exception;
}
