package babel.content.eqclasses.properties.type;


import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

public class Type extends Property
{
  protected static final String NONE = "none";
  protected static final String SRC = "src";
  protected static final String TRG = "trg";
  
  public Type()
  { m_type = EqType.NONE;
  }
  
  public Type(EqType type)
  { setType(type);
  }

  public void setType(EqType type)
  { m_type = (type == null) ? EqType.NONE : type;
  }
  
  public EqType getType()
  { return m_type;
  }
  
  public String toString()
  { return persistToString();
  }

  public String persistToString()
  { return EqType.NONE.equals(m_type) ? NONE : (EqType.SOURCE.equals(m_type) ? SRC : TRG);
  }

  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception
  {    
    if (SRC.equalsIgnoreCase(str))
    { m_type = EqType.SOURCE;
    }
    else if (TRG.equalsIgnoreCase(str))
    { m_type = EqType.TARGET;
    }
    else if (NONE.equalsIgnoreCase(str))
    { m_type = EqType.NONE;
    }
    else
    { throw new IllegalArgumentException();
    }    
  }
  
  protected EqType m_type;

  public static enum EqType {SOURCE, TARGET, NONE}
}
