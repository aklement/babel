package babel.content.eqclasses.properties.lshorder;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

public class LSHPhraseContext extends Property {

  private static final String DEFAULT_CHARSET = "UTF-8";
  private static final String SEP = " ";
  
  public LSHPhraseContext() {
    this(null, null, null, null);
  }
 
  public LSHPhraseContext(EquivalenceClass eq, byte[] beforeSig, byte[] afterSig, byte[] discSig) {
    m_eq = eq;
    m_lshBeforeSig = beforeSig;
    m_lshAfterSig = afterSig;
    m_lshDiscSig = discSig;
  }
  
  public EquivalenceClass getEq() {
    return m_eq;
  }
  
  public byte[] getBeforeSig() {
    return m_lshBeforeSig;
  }
  
  public byte[] getAfterSig() {
    return m_lshAfterSig;
  }  
  
  public byte[] getDiscSig() {
    return m_lshDiscSig;
  }
  
  public String persistToString() throws Exception {

    byte[] allSigs = new byte[m_lshBeforeSig.length + m_lshAfterSig.length + m_lshDiscSig.length]; 
    
    System.arraycopy(m_lshBeforeSig, 0, allSigs, 0, m_lshBeforeSig.length);
    System.arraycopy(m_lshAfterSig, 0, allSigs, m_lshBeforeSig.length, m_lshAfterSig.length);
    System.arraycopy(m_lshDiscSig, 0, allSigs, m_lshBeforeSig.length + m_lshAfterSig.length, m_lshDiscSig.length);
    
    return m_lshBeforeSig.length + SEP + m_lshAfterSig.length + SEP + m_lshDiscSig.length + SEP + new String(allSigs, DEFAULT_CHARSET);
  }

  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception {    
  
    m_eq = eq;

    int one = str.indexOf(SEP);
    int two = str.indexOf(SEP, one + 1);
    int three = str.indexOf(SEP, two + 1);
    
    m_lshBeforeSig = new byte[Integer.parseInt(str.substring(0, one))];
    m_lshAfterSig = new byte[Integer.parseInt(str.substring(one + SEP.length(), two))];
    m_lshDiscSig = new byte[Integer.parseInt(str.substring(two + SEP.length(), three))];

    byte[] allSigs = str.substring(three + SEP.length()).getBytes(DEFAULT_CHARSET);

    System.arraycopy(allSigs, 0, m_lshBeforeSig, 0, m_lshBeforeSig.length);
    System.arraycopy(allSigs, m_lshBeforeSig.length, m_lshAfterSig, 0, m_lshAfterSig.length);
    System.arraycopy(allSigs, m_lshBeforeSig.length + m_lshDiscSig.length, m_lshDiscSig, 0, m_lshDiscSig.length); 
  }
  
  /** Source equivalence class. */
  protected EquivalenceClass m_eq;
  /** Before LSH signature. */
  protected byte[] m_lshBeforeSig;
  /** After LSH signature. */
  protected byte[] m_lshAfterSig;
  /** Discontinuous LSH signature. */
  protected byte[] m_lshDiscSig;
}
