package babel.util.config;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Reads configuration and makes it accessible to classes that need to be configured.
 */
public class Configurator
{
  protected static final Log LOG = LogFactory.getLog(Configurator.class);
  
  /** Configuration file key / value delimeter. */
  protected static final String DELIM = " ";
  /** Comment character. */
  protected static final String COMMENT = "#";
  /** Default configuration file. */
  protected static final String DEFAULT_CONFIG = "input/default.cfg";
  /** JVM environment variable specifying config file location. */
  protected static final String ENV_CONFIG_FILE = "CONFIG";
  /** JVM environment variable specifying resource path (corpora/dictionary). */
  protected static final String ENV_RES_PATH = "RESOURCE_PATH";
  
  /** Configuration. */
  protected static HashMap<String, String> params;
  /** Configuration file name. */
  protected static String cfgFileName;
  /** Resource path. */
  protected static String resPath;
  
  /**
   * Reads configuration params from a file specified with ENV_CONFIG_FILE.
   */  
  static
  {
    cfgFileName = System.getProperty(ENV_CONFIG_FILE);
   
    if (cfgFileName == null)
    { 
      if (LOG.isWarnEnabled())
      { LOG.warn("No config file specified, assuming " + DEFAULT_CONFIG);
      }
      cfgFileName = DEFAULT_CONFIG;
    }

    resPath = System.getProperty(ENV_RES_PATH);
    
    if (resPath == null && LOG.isErrorEnabled())
    { LOG.error("No resource path specified by environment variable " + ENV_RES_PATH);
    }
    
    params = new HashMap<String, String>();
    
    try
    {
      if (LOG.isInfoEnabled())
      { LOG.info("Unpersisting from " + cfgFileName);
      }
      
      BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(cfgFileName)));
      String curLine;
      int delimIdx;
      
      // For each line - unpersist a parameter
      while ((curLine = reader.readLine()) != null)
      {  
        if (((curLine = curLine.trim()).length() != 0) && (!curLine.startsWith(COMMENT)))
        {
          delimIdx = curLine.indexOf(DELIM);
          
          if (delimIdx < 0)
          { throw new IllegalArgumentException("File " + cfgFileName + " is malformed.");
          }
  
          params.put(curLine.substring(0, delimIdx).trim(), curLine.substring(delimIdx + DELIM.length(), curLine.length()).trim());
        }
      }

      reader.close();
    } 
    catch (Exception e)
    {
      if (LOG.isErrorEnabled())
      { LOG.error("Error unpersisting from " + cfgFileName + ": " + e.toString());
      }
      
      throw new IllegalArgumentException("Error unpersisting from " + cfgFileName + ": " + e.toString());
    }
  }

  /**
   * Looks up the value of a key.
   * @param key
   * @return String value or null if the key is not found.
   */
  public static String lookup(String key)
  {
    String value;
    
    if ((value = params.get(key)) == null)
    {
      throw new IllegalArgumentException(key + " was not found");
    }
    
    return value;
  }
  
  /**
   * Dumps the (sorted) contents of configuration.
   */
  public static void dump()
  {
    ArrayList<String> keyList = new ArrayList<String>(params.keySet());
    
    Collections.sort(keyList);
    
    System.out.println(" ---------- Configuration ----------");
    for (String curKey : keyList)
    { System.out.println("  " + curKey + " : " + params.get(curKey));
    }
    System.out.println(" -----------------------------------");
  }
  
  /**
   * @return configuration file name.
   */
  public static String getConfigFileName()
  {
    return cfgFileName;
  }
  
  /**
   * @return Resource path.
   */
  public static String getResourcePath()
  {
    return resPath;
  }
}