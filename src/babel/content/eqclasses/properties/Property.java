package babel.content.eqclasses.properties;

/**
 * Superclass for property of an EquivalnceClass.
 */
public abstract class Property
{
  /**
   * @return A property ID String (a class name extending this class).
   */
  public String getPropertyId()
  { return getClass().getName();
  }
}
