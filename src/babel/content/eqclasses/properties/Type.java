package babel.content.eqclasses.properties;

public class Type extends Property
{
  protected static final String SRC = "src";
  protected static final String TRG = "trg";
      
  public Type(EqType type)
  {
    m_type = type;
  }
  
  public EqType getType()
  {
    return m_type;
  }
  
  public String toString()
  {
    return (m_type == null) ? "None" : (EqType.SOURCE.equals(m_type) ? "Source" : "Target");
  }
  
  protected EqType m_type;

  public static enum EqType {SOURCE, TARGET}

  public String persistToString()
  {
    return EqType.SOURCE.equals(m_type) ? SRC : TRG;
  }

  public boolean unpersistFromString(String str)
  {
    boolean src;
    boolean done = (src = SRC.equalsIgnoreCase(str)) || TRG.equalsIgnoreCase(str);
    
    if (done)
    { m_type = src ? EqType.SOURCE : EqType.TARGET;
    }
    
    return done;
  };
}
