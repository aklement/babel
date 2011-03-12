// Benjamin Van Durme, vandurme@cs.jhu.edu,  3 Nov 2010

package babel.util.jerboa;

/**
 * Contains routines for using bit signatures created with some variant the
 * cosine preserving locality sensitive hash.
 */
public class LSHSignature {
  // based on http://infolab.stanford.edu/~manku/bitcount/bitcount.html
  final static int bitsIn[] = {0,1,1,2,1,2,2,3,1,2,2,3,2,3,3,4,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,1,2,2,3,2,3,3,4,2,3,3,4,3,4,4,5,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,2,3,3,4,3,4,4,5,3,4,4,5,4,5,5,6,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,3,4,4,5,4,5,5,6,4,5,5,6,5,6,6,7,4,5,5,6,5,6,6,7,5,6,6,7,6,7,7,8};

  //final static double factor = Math.PI/8.0;

  public static double approximateCosine(byte[] x, byte[] y) {
    double diff = 0.0;
		
    for (int i = 0; i < x.length; i++)
      diff += bitsIn[(x[i] ^ y[i]) & 0xFF];

    return Math.cos((diff * Math.PI)/(x.length*8.0));
    //return Math.cos((diff/(1.0*x.length)) * factor);
  }
}