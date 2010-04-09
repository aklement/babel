package babel.util.persistence;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

public class EqClassPersister
{
  protected static final Log LOG = LogFactory.getLog(EqClassPersister.class);
  
  public static void persistEqClasses(Collection<EquivalenceClass> eqs, String fileName) throws IOException
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    
    for (EquivalenceClass eq : eqs)
    {
      writer.write(eq.persistToString());
      writer.newLine();
    }
    
    writer.close();
    
    if (LOG.isInfoEnabled())
    { LOG.info("Persisted " + eqs.size() + " equivalence classes.");
    }
  }
  
  public static void persistProperty(Collection<EquivalenceClass> eqs, String propId, String fileName) throws IOException
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    int numNoProp = 0;
    Property prop;
    
    for (EquivalenceClass eq : eqs)
    {
      if (null != (prop = eq.getProperty(propId)))
      {
        writer.write(Integer.toString(eq.getId()));
        writer.write("\t[");
        writer.write(prop.persistToString());
        writer.write("]");
        writer.newLine();  
      }
      else
      { numNoProp++;
      }
    }
    
    writer.close();
    
    if (LOG.isInfoEnabled())
    { LOG.info("Persisted property " + propId + " for " + (eqs.size() - numNoProp) + " eq. classes, " + numNoProp + " did not have the property.");
    }
  }
}
