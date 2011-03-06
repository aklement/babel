package babel.content.eqclasses.properties;

import babel.content.eqclasses.EquivalenceClass;

public abstract class LSHProperty extends Property {
  
  private static final String DEFAULT_CHARSET = "UTF-8";
  
  public LSHProperty() {
    this(null, null);
  }
 
  public LSHProperty(EquivalenceClass eq, byte[] signature) {
    m_eq = eq;
    m_lshSignature = signature;
  }
  
  public EquivalenceClass getEq() {
    return m_eq;
  }
  
  public byte[] getSignature() {
    return m_lshSignature;
  }

  public String persistToString() throws Exception {
    return new String(m_lshSignature, DEFAULT_CHARSET);
  }

  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception {
    m_eq = eq;
    m_lshSignature = str.getBytes(DEFAULT_CHARSET);
  }
  
  /** Source equivalence class. */
  protected EquivalenceClass m_eq;
  /** LSH Signature. */
  protected byte[] m_lshSignature;
}
