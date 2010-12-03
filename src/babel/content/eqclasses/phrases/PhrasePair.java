package babel.content.eqclasses.phrases;

public class PhrasePair {

  public PhrasePair(Phrase srcPhrase, Phrase trgPhrase) {
    m_srcPhrase = srcPhrase;
    m_trgPhrase = trgPhrase;
  }
  
  public Phrase srcPhrase() {
    return m_srcPhrase;
  }
  
  public Phrase trgPhrase() {
    return m_trgPhrase;
  }
  
  protected Phrase m_srcPhrase;
  protected Phrase m_trgPhrase;
}
