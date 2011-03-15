package babel.reordering.scorers;

import babel.content.eqclasses.phrases.Phrase;
import babel.content.eqclasses.properties.lshorder.LSHPhraseContext;

import babel.util.jerboa.LSH;

public class LSHMonoScorer extends ReorderingScorer {

  public OrderTriple scoreBefore(Phrase srcPhrase, Phrase trgPhrase) {
    LSHPhraseContext srcLSHContext = (LSHPhraseContext)srcPhrase.getProperty(LSHPhraseContext.class.getName());
    LSHPhraseContext trgLSHContext = (LSHPhraseContext)trgPhrase.getProperty(LSHPhraseContext.class.getName());
    
    if (srcLSHContext == null || trgLSHContext == null) {
      throw new IllegalArgumentException("At least one of the classes has no property " + LSHPhraseContext.class.getName() + ".");
    }
    
    byte[] srcBeforeSig = srcLSHContext.getBeforeSig();
    byte[] trgBeforeSig = trgLSHContext.getBeforeSig();
    byte[] trgAfterSig = trgLSHContext.getAfterSig();
    byte[] trgDiscSig = trgLSHContext.getDiscSig();

    double monoFeat = Math.max(0.0, LSH.scoreSignatures(srcBeforeSig, trgBeforeSig));    
    double swapFeat = Math.max(0.0, LSH.scoreSignatures(srcBeforeSig, trgAfterSig));    
    double discFeat = Math.max(0.0, LSH.scoreSignatures(srcBeforeSig, trgDiscSig));    
    
    return new OrderTriple(monoFeat, swapFeat, discFeat);
  }

  public OrderTriple scoreAfter(Phrase srcPhrase, Phrase trgPhrase) {
    LSHPhraseContext srcLSHContext = (LSHPhraseContext)srcPhrase.getProperty(LSHPhraseContext.class.getName());
    LSHPhraseContext trgLSHContext = (LSHPhraseContext)trgPhrase.getProperty(LSHPhraseContext.class.getName());
    
    if (srcLSHContext == null || trgLSHContext == null) {
      throw new IllegalArgumentException("At least one of the classes has no property " + LSHPhraseContext.class.getName() + ".");
    }
    
    byte[] srcAfterSig = srcLSHContext.getAfterSig();
    byte[] trgAfterSig = trgLSHContext.getAfterSig();
    byte[] trgBeforeSig = trgLSHContext.getBeforeSig();
    byte[] trgDiscSig = trgLSHContext.getDiscSig();

    double monoFeat = Math.max(0.0, LSH.scoreSignatures(srcAfterSig, trgAfterSig));    
    double swapFeat = Math.max(0.0, LSH.scoreSignatures(srcAfterSig, trgBeforeSig));    
    double discFeat = Math.max(0.0, LSH.scoreSignatures(srcAfterSig, trgDiscSig));    
    
    return new OrderTriple(monoFeat, swapFeat, discFeat);
  }
}
