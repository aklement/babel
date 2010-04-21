package babel.content.eqclasses.properties;

import java.io.PrintStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import babel.content.eqclasses.EquivalenceClass;

public class TimeDistribution extends Property implements Cloneable
{
  /**
   * Returns a non-normalized uniform TimeDistribution.
   * 
   * @param numWindows
   * @return
   */
  public static TimeDistribution getUniformDistribution(int numWindows)
  {
    return new TimeDistribution(numWindows);
  }
  
  /**
   * Creates a special purpose uniform TimeDistribution.
   * @param numWindows number of windows in the distribution
   */
  protected TimeDistribution(int numWindows)
  {
    super();
    
    m_windows = new ArrayList<Double>(numWindows);
    m_count = numWindows;
    m_normalized = false;
    m_sumSquares = m_sum = 0;
    
    for (int i = 0; i < numWindows; i++)
    { m_windows.add(new Double(1.0));
    }
  }
  
  /**
   * Creates a distribution object for a given equivalence class.
   * @param entity
   */
  public TimeDistribution()
  {
    super();
    
    m_windows = new ArrayList<Double>();
    m_count = 0;
    m_normalized = false;
    m_sumSquares = m_sum = 0;
  }
  
  /**
   * Adds the next counting bin - for use by DistributionCollectors.
   * @param count num of occurences in the bin
   */
  public void addBin(int count)
  {
    if (m_normalized)
    { throw new IllegalStateException("TimeDistribution is normalized: cannot add more bins.");
    }
    
    m_windows.add(new Double(count));
    m_count += count;
  }
  
  /**
   * Dumps distribution to the provided PrintStream.
   * @param out
   */
  public void dumpDistribution(PrintStream out)
  {
    out.println("--- Time Distribution ---");
    
    for (int i = 0; i < m_windows.size(); i++)
    { 
      out.println(i + " : " + (Double)m_windows.get(i));
    }
    
    out.println(toString());
  }
  
  /**
   * Returns short description of the Distribution.
   */
  public String toString()
  {
    return "Distribution: bins = " + m_windows.size() + ", sum = " + m_sum + ", sum of squares = " + m_sumSquares + ", occurences = " + m_count + ", normalized = " + m_normalized;
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

      for (int i = 0; (m_count != 0) && (i < m_windows.size()); i++)
      {
        curValue = ((Double)m_windows.get(i)).doubleValue() / (double)m_count;
        m_windows.set(i, new Double(curValue));
        
        m_sum += curValue;
        m_sumSquares += curValue*curValue;
      }
      
      m_normalized = true;
    }
  }
  
  /**
   * Resizes the time distribution. If new size is larger than current, adds empty windows. 
   * Else if smaller, remove windows from the tail of the window vector.
   * 
   * @param newSize
   */
  public void setSizeCrop(int newSize)
  {
    if ((newSize != m_windows.size()) && m_normalized)
    { throw new IllegalStateException("Must call before normalization");
    }
    else if (newSize < 0)
    { throw new IllegalArgumentException("Size is negative");
    }
        
    int curSize = m_windows.size();
    
    if (newSize > curSize)
    { // Add windows
      for (int i = 0; i < newSize - curSize; i++)
      { addBin(0);
      }
    }
    else if (newSize < curSize)
    { // Remove windows
      for (int i = 0; i < curSize - newSize; i++)
      { m_count -= ((Double)m_windows.remove(m_windows.size() - 1)).doubleValue();
      }
    }
  }

  /**
   * Resizes the time distribution. Stretches/shrinks counts appropriately.
   * 
   * @param newSize
   */
  public void setSizeScale(int newSize)
  {
    if ((newSize != m_windows.size()) && m_normalized)
    { throw new IllegalStateException("Must call before normalization");
    }
    else if (newSize < 0)
    { throw new IllegalArgumentException("Size is negative");
    }

    if (m_windows.size() != newSize)
    {
      // If > 1 - stretch
      double scale = (double)m_windows.size() / (double)newSize;
      double srcRangeFrom, srcRangeTo;
      int srcIdxFrom, srcIdxTo;
      double srcWeightedCount = 0;
      double curSrcWeight;
      ArrayList<Double> newWindows = new ArrayList<Double>(newSize);
    
      // Go through all the new bins
      for (int curIdx = 0; curIdx < newSize; curIdx++)
      {
        // Compute raw range to the source array
        srcRangeFrom = scale * curIdx;
        srcRangeTo = scale * (curIdx + 1);
      
        // Comute indices enaompasing the range
        srcIdxFrom = (int)Math.floor(srcRangeFrom); 
        srcIdxTo = (int)Math.ceil(srcRangeTo);

        // Cumulative weighted count
        srcWeightedCount = 0;
      
        // For each new bin, collect prorated counts from old bins that map to them
        for (int curSrcIdx = srcIdxFrom; curSrcIdx < srcIdxTo; curSrcIdx++)
        {
          // Compute weight - the size of the overlap between the current target and source windows
          curSrcWeight = Math.min(curSrcIdx + 1, srcRangeTo) - Math.max(curSrcIdx, srcRangeFrom);
        
          // Get a count from the source window and pro-rate it by the size of overlap
          srcWeightedCount += ((Double)m_windows.get(curSrcIdx)).doubleValue() * curSrcWeight;
        }
      
        newWindows.add(new Double(srcWeightedCount));
      }
    
      m_windows.clear();
      m_windows = newWindows;
    }
  }
  
  /**
   * @return counting bins
   */
  public List<Double> getBins()
  {
    return m_windows;
  }
  
  /**
   * @return number of bins
   */
  public int getSize()
  {
    return m_windows.size();
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
  public int getTotalOccurences()
  {
    return m_count;
  }
 
  /**
   * Merges two distributions in the following way: if for a particular cell,
   * the count of either of the is zero, the aggregate is zero, otherise, the
   * aggregate is maximum of the two.
   * @param one normalized TimeDistribution.
   * @param two normalized TimeDistribution of the same size as one.
   * @return merged distribution.
   */
  public static TimeDistribution mergeDistros(TimeDistribution one, TimeDistribution two)
  {
    if (!one.m_normalized || !two.m_normalized || (one.m_windows.size() != two.m_windows.size()))
    {
      throw new IllegalArgumentException("Both distros must be normalized and of the same size.");
    }
    
    TimeDistribution distro = new TimeDistribution();
    int countOne;
    int countTwo;
    int totalCount;
    
    // Populate the new distro's counts
    for (int curWnd = 0; curWnd < one.m_windows.size(); curWnd++)
    {
      countOne = (int)Math.round(((Double)one.m_windows.get(curWnd)).doubleValue() * one.m_count);
      countTwo = (int)Math.round(((Double)two.m_windows.get(curWnd)).doubleValue() * two.m_count);
      
      totalCount = (countOne == 0 || countTwo == 0) ? 0 : Math.max(countOne, countTwo);
      //totalCount = countOne + countTwo;

      distro.m_windows.add(new Double(totalCount));
      distro.m_count += totalCount;
    }
    
    // Normalize it
    distro.normalize();
    
    return distro;
  }
  
  /**
   * Returns a clone of the current object (short of the transform, which will be evaluated lazily).
   */
  public Object clone()
  {
    TimeDistribution distro = new TimeDistribution();

    for (int i = 0; i < m_windows.size(); i++)
    { distro.m_windows.add(new Double(((Double)m_windows.get(i)).doubleValue()));
    }
    
    distro.m_count = m_count;
    distro.m_normalized = m_normalized;
    distro.m_sum = m_sum;
    distro.m_sumSquares = m_sumSquares;
    
    return distro;
  }
  
  @Override
  public String persistToString()
  {
    StringBuilder strBld = new StringBuilder();

    strBld.append(m_sum); strBld.append("\t");
    strBld.append(m_sumSquares); strBld.append("\t");
    strBld.append(m_count); strBld.append("\t");
    strBld.append(m_normalized);
    
    for (Double windowCount : m_windows)
    {
      strBld.append("\t");
      strBld.append(windowCount);
    }
    
    return strBld.toString();
  }

  @Override
  public void unpersistFromString(EquivalenceClass eq, Map<Integer, EquivalenceClass> allEqs, String str) throws Exception
  {
    String[] toks = str.split("\t");
    
    m_sum = Double.parseDouble(toks[0]);
    m_sumSquares = Double.parseDouble(toks[1]);
    m_count = Integer.parseInt(toks[2]);
    m_normalized = Boolean.parseBoolean(toks[3]);
    
    m_windows.clear();
    for (int i = 4; i < toks.length; i++)
    { m_windows.add(Double.parseDouble(toks[i]));
    }
  }
  
  /** Time windows containing counts per window. */
  protected ArrayList<Double> m_windows;
  /** Count of total occurences. */
  protected int m_count;
  protected boolean m_normalized;
  protected double m_sum;
  protected double m_sumSquares;
}
