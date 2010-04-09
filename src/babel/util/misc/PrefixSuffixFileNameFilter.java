package babel.util.misc;

import java.io.File;
import java.io.FilenameFilter;

public class PrefixSuffixFileNameFilter implements FilenameFilter
{
  public PrefixSuffixFileNameFilter()
  { 
    this(null, null);
  }
  
  public PrefixSuffixFileNameFilter(String prefix, String suffix)
  { 
    super();
    m_prefix = prefix;
    m_suffix = suffix;
  }
  
  public void setPrefix(String prefix)
  {
    m_prefix = prefix;
  }
  
  public void setSuffix(String suffix)
  {
    m_suffix = suffix;
  }
  
  public boolean accept(File dir, String name)
  {
    boolean starts = (m_prefix == null) || name.startsWith(m_prefix);
    boolean ends = (m_suffix == null) || name.endsWith(m_suffix);
    
    return starts && ends; 
  }
  
  protected String m_prefix;
  protected String m_suffix;
}