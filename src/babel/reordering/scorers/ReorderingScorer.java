package babel.reordering.scorers;

import babel.content.eqclasses.phrases.Phrase;

public abstract class ReorderingScorer {

  /** 
   * Estimates the reordering features with the preceding phrase. Note: Must be 
   * able to handle concurrent calls.
   */
  public abstract OrderTriple scoreBefore(Phrase srcPhrase, Phrase trgPhrase);
  
  /** 
   * Estimates the reordering features with the following phrase. Note: Must be 
   * able to handle concurrent calls.
   */
  public abstract OrderTriple scoreAfter(Phrase srcPhrase, Phrase trgPhrase);

  public String toString()
  { return getClass().getName();
  }
  
  public class OrderTriple {
    
    public OrderTriple(double monoScore, double swapScore, double discScore) {
      m_monoScore = monoScore;
      m_swapScore = swapScore;
      m_discScore = discScore;
    }
    
    public double getMonoScore() {
      return m_monoScore;
    }

    public double getSwapScore() {
      return m_swapScore;
    }
    
    public double getDiscScore() {
      return m_discScore;
    }    
    
    private double m_monoScore;
    private double m_swapScore;
    private double m_discScore;
  }
}