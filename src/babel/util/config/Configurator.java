package babel.util.config;

import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.XMLConfiguration;

/**
 * Reads configuration and makes it accessible to classes that need to be configured.
 */
public class Configurator
{
  protected static final Log LOG = LogFactory.getLog(Configurator.class);

  /** JVM environment variable specifying config file location. */
  protected static final String ENV_CONFIG_FILE = "CONFIG";
  /** Default configuration file. */  
  protected static final String DEFAULT_CONFIG = "babel.xml";
  /** Configuration. */
  
  public static HierarchicalConfiguration CONFIG;
  
  static
  {

    String cfgFileName = System.getProperty(ENV_CONFIG_FILE);
    
    if (cfgFileName == null)
    { 
      if (LOG.isInfoEnabled())
      { LOG.info("No config file specified, assuming " + DEFAULT_CONFIG);
      }
      
      cfgFileName = DEFAULT_CONFIG;
    }

    try
    {
      CONFIG = new XMLConfiguration(cfgFileName);
    }
    catch (Exception e)
    {
      if (LOG.isErrorEnabled())
      { LOG.error("Error reading configuration from file " + cfgFileName + ".");
      }
      
      CONFIG = null;      
    }
  }
  
  @SuppressWarnings("unchecked")
  public static String getConfigDescriptor()
  {
    StringBuilder strBld = new StringBuilder(); 
    Iterator<String> iter = CONFIG.getKeys();
    String key;
    List<String> vals;
   
    strBld.append("----------- Configuration -----------\n");
    
    while (iter.hasNext())
    {
      key = iter.next();
      vals = CONFIG.getList(key);
      
      for (String val : vals)
      { strBld.append(key + ": " + val + "\n");
      }
    }

    strBld.append("-------------------------------------");
    
    return strBld.toString();
  }
}