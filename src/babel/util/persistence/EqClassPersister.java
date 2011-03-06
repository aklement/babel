package babel.util.persistence;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.content.eqclasses.properties.Property;

public class EqClassPersister
{
  protected static final Log LOG = LogFactory.getLog(EqClassPersister.class);
  protected static final String DELIM_OPEN = "\t[";
  protected static final String DELIM_CLOSE = "]";
    
  public static void persistEqClasses(Set<EquivalenceClass> eqs, String fileName) throws IOException
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
  
  public static Set<EquivalenceClass> unpersistEqClasses(Class<? extends EquivalenceClass> eqClassClass, String fileName) throws Exception
  {
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    String line;
    EquivalenceClass eq;
    HashSet<EquivalenceClass> eqs = new HashSet<EquivalenceClass>();
    
    while ((line = reader.readLine()) != null)
    {
      eq = eqClassClass.newInstance();
      eq.unpersistFromString(line);
      eqs.add(eq);
    }

    reader.close();
    
    if (LOG.isInfoEnabled())
    { LOG.info("Unpersisted " + eqs.size() + " equivalence classes from " + fileName);
    }
    
    return eqs;
  }
  
  public static void persistProperty(Set<? extends EquivalenceClass> eqs, String propClassName, String fileName) throws Exception
  {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    int numNoProp = 0;
    Property prop;
    
    for (EquivalenceClass eq : eqs)
    {
      if (null != (prop = eq.getProperty(propClassName)))
      {
        writer.write(Long.toString(eq.getId()));
        writer.write(DELIM_OPEN);
        writer.write(prop.persistToString());
        writer.write(DELIM_CLOSE);
        writer.newLine();  
      }
      else
      { numNoProp++;
      }
    }
    
    writer.close();
    
    if (LOG.isInfoEnabled())
    { LOG.info("Persisted property " + propClassName + " for " + (eqs.size() - numNoProp) + " eq. classes, " + numNoProp + " did not have the property.");
    }
  }

  @SuppressWarnings("unchecked")
  public static void unpersistProperty(Set<? extends EquivalenceClass> eqs, String propClassName, String fileName) throws Exception
  {
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    HashMap<Long, EquivalenceClass> eqMap = new HashMap<Long, EquivalenceClass>();
    int numWithProp = 0;
    Property prop;
    EquivalenceClass eq;
    String line;
    int idxOpen, idxClose;
    Class<? extends Property> propClass = (Class<? extends Property>)Class.forName(propClassName);
        
    for (EquivalenceClass curEq : eqs)
    { eqMap.put(curEq.getId(), curEq);
    }
    
    while ((line = reader.readLine()) != null)
    {
      idxOpen = line.indexOf(DELIM_OPEN);
      idxClose = line.lastIndexOf(DELIM_CLOSE);
      
      eq = eqMap.get(Long.parseLong(line.substring(0, idxOpen)));

      prop = propClass.newInstance();
      prop.unpersistFromString(eq, line.substring(idxOpen + DELIM_OPEN.length(), idxClose));
      
      eq.setProperty(prop);
      numWithProp++;
    }
    
    reader.close();
    
    if (LOG.isInfoEnabled())
    { LOG.info("Unpersisted property " + propClass.getSimpleName() + " for " + numWithProp + " eq. classes, " +  (eqs.size() - numWithProp) + " did not have the property.");
    }
  }

}