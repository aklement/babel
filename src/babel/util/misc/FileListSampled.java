package babel.util.misc;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FilenameFilter;
import java.io.InputStream;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Arrays;
import java.util.Random;

public class FileListSampled extends FileList implements Enumeration<InputStream>
{
  public FileListSampled(String dir, double sampleRate) 
  {
    this(dir, null, sampleRate);
  }
  
  public FileListSampled(String dir, String[] filtertitles) 
  {
    this(dir, null, filtertitles);
  }
  
  public FileListSampled(String dir, FilenameFilter nameFilter, double sampleRate) 
  {
	super(dir, nameFilter);
    m_nameFilter = nameFilter;
    m_dir = new File(dir);
    m_current = 0;
    m_sampleRate=sampleRate;
    m_generator = new Random();  
    m_useTitleFilter=false;
    m_fileTitles = new HashMap<String,Integer>();
    //System.out.println("New FileListSampled; using sample rate of "+sampleRate);
  }

  public FileListSampled(String dir, FilenameFilter nameFilter, String[] filtertitles) 
  {
	  super(dir, nameFilter);
    m_nameFilter = nameFilter;
    m_dir = new File(dir);
    m_current = 0;
    m_sampleRate=1.0;
    m_useTitleFilter = true;
    for (String mytitle : filtertitles){
    	m_titleFilter.put(mytitle, 1);
    }
	  //System.out.println("New FileListSampled; filtering by "+filtertitles.length+" titles");
  }
  
  
  public String getDir()
  {
    return m_dir.getAbsolutePath();
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
    m_listOfFiles = (m_nameFilter == null) ? m_dir.list() : m_dir.list(m_nameFilter);
    m_current = 0;
    
    for (int i = 0; (m_listOfFiles != null) && (i < m_listOfFiles.length); i++)
    {
      m_listOfFiles[i] = m_dir.getAbsolutePath() + File.separator + m_listOfFiles[i];
    }
  }

  public void gather(int depth)
  {
    m_listOfFiles = null;
    m_current = 0;

    LinkedList<String> list = recursiveGather(depth, m_dir);
    
    if (list.size() != 0)
    {
      list.toArray(m_listOfFiles = new String[list.size()]);
    }
    //First time after gathered, always limit to that list of files
    m_useTitleFilter=true;
  }
  
  protected LinkedList<String> recursiveGather(int depth, File parent)
  {
    LinkedList<String> curList = new LinkedList<String>();
    
    if (depth > 0)
    {      
      for (File file : parent.listFiles())
      {
        if (file.isDirectory())
        {
          curList.addAll(recursiveGather(depth - 1, file));
        }
        else if ((m_nameFilter == null) || (m_nameFilter.accept(parent, file.getName()))) //If matches regex
        {
        	if (m_useTitleFilter){ //If using sample from another (e.g. src done first) time
        		if (m_titleFilter.containsKey(file.getName().substring(0, file.getName().length()-3))){
            	    curList.add(parent.getAbsolutePath() + File.separator + file.getName()); //Remember page title        			
            	    m_fileTitles.put(file.getName().substring(0, file.getName().length()-3), 1);
            	    //System.out.println("Already sampled file: "+file.getName().substring(0,file.getName().length()-3));
        		}
        		//else{
        			//System.out.println("Don't want to keep this one: "+file.getName().substring(0,file.getName().length()-3));
        		//}
        	}
        	else{ //If sampling for the first time
        	  double rando=m_generator.nextDouble();
                if (rando<m_sampleRate){        	 // Randomly add w/ samplingRate probability
            	    curList.add(parent.getAbsolutePath() + File.separator + file.getName()); //Remember page title
            	    m_fileTitles.put(file.getName().substring(0, file.getName().length()-3), 1);
            	    m_titleFilter.put(file.getName().substring(0, file.getName().length()-3), 1);
            	    //System.out.println("Sampled file: "+file.getName().substring(0, file.getName().length()-3));
                }
              }
          }
      }
    }
    
    return curList;
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
  
  /**
   * Return named list of files, if there is one
   */
  public String[] getNamedFileNames(){
	  String[] files = new String[400000];
	  int i=0;
	  for (String k : m_fileTitles.keySet()){
		  files[i]=k;
		  i++;
	  }
	  return files;
  }

  
  public HashMap<String, Integer> m_fileTitles = new HashMap<String,Integer>();
  public HashMap<String,Integer> m_titleFilter = new HashMap<String,Integer>();
  public Boolean m_useTitleFilter;
  protected String[] m_listOfFiles;
  protected int m_current;
  protected File m_dir;
  protected FilenameFilter m_nameFilter;
  protected double m_sampleRate;
  protected Random m_generator;
}
