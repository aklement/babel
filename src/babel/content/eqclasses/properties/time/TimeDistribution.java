package babel.content.eqclasses.properties.time;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

public class TimeDistribution extends Property implements Cloneable
{
  /**
   * Returns an unnormalized uniform TimeDistribution.
   * 
   * @param numWindows
   * @return
   */
  public static TimeDistribution getUniformDistribution(int numWindows)
  {
    TimeDistribution distro = new TimeDistribution();
    
    distro.m_count = numWindows;

    for (int i = 0; i < numWindows; i++)
    { distro.m_bins.put(i, new Double(1.0));
    }
    
    return distro;
  }
  
  /**
   * Creates a distribution object for a given equivalence class.
   * @param entity
   */
  public TimeDistribution()
  {
    super();
    
    m_bins = new HashMap<Integer, Double>();
    m_count = 0;
    m_normalized = false;
    m_sumSquares = m_sum = 0;
  }
  
  public void removeBin(int binIdx)
  {
    if (m_normalized)
    { throw new IllegalStateException("TimeDistribution is normalized: cannot remove bins.");
    }
    
    Double count = m_bins.remove(binIdx);
    
    if (count != null)
    { m_count -= count;
    }
  }

  public void removeBins(Set<Integer> binIdxs)
  {
    if (m_normalized)
    { throw new IllegalStateException("TimeDistribution is normalized: cannot remove bins.");
    }
    
    Double count;
    
    for (Integer binIdx : binIdxs)
    {
      if (null != (count = m_bins.remove(binIdx)))
      { m_count -= count;
      }
    }
  }
  
  /**
   * Adds the next counting bin - for use by DistributionCollectors.
   * @param binIdx bin index [0, m_maxBinCount].
   * @param count num of occurences in the bin
   */
  public void addBin(int binIdx, int count)
  {
    if (m_normalized)
    { throw new IllegalStateException("TimeDistribution is normalized: cannot add more bins.");
    }
   
    if (count != 0)
    {
      m_bins.put(binIdx, new Double(count));
      m_count += count;
    }
  }
  
  /**
   * Returns short description of the Distribution.
   */
  public String toString()
  {
    return "Distribution: bins = " + m_bins.size() + ", sum = " + m_sum + ", sum of squares = " + m_sumSquares + ", occurences = " + m_count + ", normalized = " + m_normalized;
  }
  
  /**
   * @return true iff the distro was normalized.
   */
  public boolean isNormalized()
  {
    return m_normalized;
  }
  
  /**
   * Normalizes the distribution.
   */
  public void normalize()
  {
    if (!m_normalized)
    {
      double curValue = 0.0;
      m_sumSquares = m_sum = 0.0;

      if (m_count != 0)
      {
        for (Integer key : m_bins.keySet())
        {
          curValue = m_bins.get(key) / (double)m_count;
          m_bins.put(key, new Double(curValue));
          
          m_sum += curValue;
          m_sumSquares += curValue*curValue;
        }
      }
      
      m_normalized = true;
    }
  }
  
  /**
   * @return counting bins
   */
  public HashMap<Integer, Double> getBins()
  {
    return m_bins;
  }
  
  /**
   * @return number of bins
   */
  public int getSize()
  {
    return m_bins.size();
  }
  
  /**
   * @return sum of the normalized distro
   */
  public double getSum()
  {
    return m_sum;
  }

  /**
   * @return sum of the squares of the normalized distro
   */
  public double getSumSquares()
  {
    return m_sumSquares;
  }
  
  /**
   * @return total count
   */
  public long getTotalOccurences()
  {
    return m_count;
  }
 
  public void reBin(int binSize, boolean slidingWindow)
  {
    if (m_normalized)
    { throw new IllegalArgumentException("Distribution must not be be normalized.");
    }
    if (binSize <= 0)
    { throw new IllegalArgumentException("Bin size must be positive.");      
    }
    if (binSize == 1)
    { return;
    }
    
    int binStartIdx = 0;
    double newBinCount = 0;
    int newBinIdx = 0;
    int newTotalCount = 0;
    Double count;
    
    HashMap<Integer, Double> newBins = new HashMap<Integer, Double>();
    HashSet<Integer> seenBins = new HashSet<Integer>();
    
    while (m_bins.size() > seenBins.size())
    {
      newBinCount = 0;
      
      // Count up counts in current window
      for (int i = 0; i < binSize; i++)
      {
        if (null != (count = m_bins.get(binStartIdx + i)))
        {
          seenBins.add(binStartIdx + i);
          newBinCount += count;
        }
      }
      
      if (newBinCount > 0)
      {
        newBins.put(newBinIdx, newBinCount);
        newTotalCount += newBinCount;
      }
      
      newBinIdx++;
      binStartIdx += slidingWindow ? 1 : binSize;
    }
    
    m_bins = newBins;
    m_count = newTotalCount;
  }
  
  public static TimeDistribution mergeDistros(TimeDistribution one, TimeDistribution two)
  {
    if (one.m_normalized || two.m_normalized)
    { throw new IllegalArgumentException("Distros must not be be normalized.");
    }
    
    TimeDistribution distro = new TimeDistribution();
    HashSet<Integer> allKeys = new HashSet<Integer>(one.m_bins.keySet());
    allKeys.addAll(two.m_bins.keySet());
    Double curKeyCount;
    double keyCount;
    
    for (Integer key : allKeys)
    {
      curKeyCount = one.m_bins.get(key);
      keyCount = (curKeyCount == null) ? 0 : curKeyCount;

      curKeyCount = two.m_bins.get(key);
      keyCount += (curKeyCount == null) ? 0 : curKeyCount;
      
      distro.m_bins.put(key, new Double(keyCount));
    }

    distro.m_count = one.m_count + two.m_count;
    
    return distro;
  }
  
  public String persistToString()
  {
    StringBuilder strBld = new StringBuilder();

    strBld.append(m_sum); strBld.append("\t");
    strBld.append(m_sumSquares); strBld.append("\t");
    strBld.append(m_count); strBld.append("\t");
    strBld.append(m_normalized);
    
    ArrayList<Integer> keys = new ArrayList<Integer>(m_bins.keySet());
    Collections.sort(keys);
    
    for (Integer key : keys)
    {
      strBld.append("\t");
      strBld.append(key + ":" + m_bins.get(key));      
    }
    
    return strBld.toString();
  }

  @Override
  public void unpersistFromString(EquivalenceClass eq, String str) throws Exception
  {
    String[] toks = str.split("\t");
    int idx;
    
    m_sum = Double.parseDouble(toks[0]);
    m_sumSquares = Double.parseDouble(toks[1]);
    m_count = Long.parseLong(toks[2]);
    m_normalized = Boolean.parseBoolean(toks[3]);
    
    m_bins.clear();
    for (int i = 4; i < toks.length; i++)
    { 
      idx = toks[i].indexOf(':');
      m_bins.put(Integer.parseInt(toks[i].substring(0, idx)), Double.parseDouble(toks[i].substring(idx + 1)));
    }
  }
  
  /** Time windows containing counts per window. */
  protected HashMap<Integer, Double> m_bins;
  /** Count of total occurences. */
  protected long m_count;
  protected boolean m_normalized;
  protected double m_sum;
  protected double m_sumSquares;
}
