package babel.util.misc;

import java.io.File;
import java.io.FilenameFilter;
import java.util.regex.Pattern;

public class RegExFileNameFilter implements FilenameFilter
{

  public RegExFileNameFilter()
  { 
    this(null);
  }
  
  public RegExFileNameFilter(String regEx)
  { 
    super();
    m_regEx = regEx;
  }
  
  public void setRegEx(String regEx)
  {
    m_regEx = regEx;      
  }
  
  public boolean accept(File dir, String name)
  {
    return (m_regEx == null) || Pattern.matches(m_regEx, name);
  }
  
  protected String m_regEx;
}
