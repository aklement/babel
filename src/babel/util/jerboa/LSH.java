package babel.util.jerboa;

import java.util.HashMap;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Builds / compares signature vectors. 
 * Modified code of Benjamin Van Durme, vandurme@cs.jhu.edu
 */
public class LSH {

  private static final Log LOG = LogFactory.getLog(LSH.class);  
  /** Number of bits (b) */
  private static final int NUM_BITS = 256;//4096;//256;
  /**Size of the pool. */
  private static final int POOL_SIZE = 100000;//10000000;//100000;
 
  public LSH() {
    try {
      m_hashes = Hash.getRandomHashes(NUM_BITS);
      m_pool = new double[POOL_SIZE];
      
      Random random = new Random();
      
      for (int i = 0; i < m_pool.length; i++) {
        m_pool[i] = random.nextGaussian();
      }
    } catch (Exception e) {
      m_hashes = null;
      LOG.error("Failed to instantiate class", e);
    }
  }

  public byte[] buildSignature(HashMap<String, Double> features) {
    
    float[] sumArray = new float[NUM_BITS];
    byte[] sig = new byte[NUM_BITS/8];

    // Generate the counter array
    for (String feature : features.keySet()) {
      
      for (int i = 0; i < NUM_BITS; i++) {
        sumArray[i] += features.get(feature) * m_pool[Hash.hash(feature, m_hashes[i], m_pool.length)];
      }
    }
    
    // Build the signature
    int s,i,j;

    for (i = 0; i < NUM_BITS; i+=8) {
      s = 0;
          
      if (sumArray[i] > 0) {
        s = s | 1;
      }
 
      for (j = 1; j < 8; j++) {
        s = s << 1;
        if (sumArray[i+j] > 0) {
          s = s | 1;
        }
      }
          
      sig[i/8] = (byte)s;
    }
    
    return sig;
  }
  
  public static double scoreSignatures(byte[] sigX, byte[] sigY) {
    return LSHSignature.approximateCosine(sigX, sigY);
  }
  
  /** Pool of random numbers */
  private double[] m_pool;
  /** Hashes. */
  private int[] m_hashes;
}