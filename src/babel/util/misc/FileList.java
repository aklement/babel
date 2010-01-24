package babel.util.misc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;

import java.util.Enumeration;
import java.util.NoSuchElementException;
import java.util.Arrays;

public class FileList implements Enumeration<InputStream>
{
  public FileList(String directory, FilenameFilter nameFilter) 
  {
    m_nameFilter = nameFilter;
    m_dir = new File(directory);
    m_current = 0;
  }
  
  public boolean hasMoreElements() 
  {
    return (m_listOfFiles != null) && (m_current < m_listOfFiles.length);
  }
  
  public InputStream nextElement()
  {
    InputStream in = null;
    
    if (!hasMoreElements())
    { throw new NoSuchElementException("No more files.");
    }
    else 
    {
      String nextElement = m_listOfFiles[m_current++];
      
      try 
      { 
        in = new FileInputStream(nextElement);
      } 
      catch (FileNotFoundException e)
      { 
        System.err.println("ListOfFiles: Can't open " + nextElement);
      }
    }
    
    return in;
  }
  
  public String[] getFileNames()
  {
    return m_listOfFiles;
  }
  
  public String getFileName(int index)
  {
    return ((m_listOfFiles != null) && (index >= 0) && (index < m_listOfFiles.length)) ? m_listOfFiles[index] : null;
  }
  
  public int size()
  {
    return (m_listOfFiles == null) ? -1 : m_listOfFiles.length;
  }
  
  public void gather()
  {
    m_listOfFiles = m_dir.list(m_nameFilter);
    m_current = 0;
    
    for (int i = 0; (m_listOfFiles != null) && (i < m_listOfFiles.length); i++)
    {
      m_listOfFiles[i] = m_dir.getAbsolutePath() + File.separator + m_listOfFiles[i];
    }
  }
  
  /**
   * Sorts constituent files in lexicographic order.
   */
  public void sort()
  {
    if (m_listOfFiles != null)
    { Arrays.sort(m_listOfFiles);
    }
  }
  
  protected String[] m_listOfFiles;
  protected int m_current;
  protected File m_dir;
  protected FilenameFilter m_nameFilter;
}
