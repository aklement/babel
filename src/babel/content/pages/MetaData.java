/**
 * This file is licensed to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package babel.content.pages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.nutch.metadata.Metadata;

import babel.util.persistence.XMLPersistable;

/**
 * Stores key-value matadata (allowing for multiple values per key).
 */
public class MetaData implements XMLPersistable, Writable
{
  private static final String XML_TAG_METADATA = "MetaData";  
  private static final String XML_ATTRIB_TYPE = "Type";
  
  public MetaData()
  {
    this(null);
  }
  
  public MetaData(String typeLabel)
  {
    m_typeLabel = (typeLabel == null) ? new String() : typeLabel;
    m_metadata = new Metadata();
  }
  
  public MetaData(String typeLabel, Metadata nutchMeta)
  {
    this(typeLabel);
    setAll(nutchMeta);
  }
  
  /**
   * Associates a given value with a given key. Previous value(s) associated
   * with the key are removed. 
   */
  public void set(String key, String value)
  {
    if (key != null && value != null && key.length() > 0 && value.length() > 0)
    {
      m_metadata.set(key, value);
    }    
  }
  
   /**
   * Adds a given key / value pair. 
   */
  public void add(String key, String value)
  {    
    if (key != null && value != null && key.length() > 0 && value.length() > 0)
    { m_metadata.add(key, value);
    }
  }
  
  public void add(String key, String[] values)
  {
    if (key != null && values != null && key.length() > 0 && values.length > 0)
    {
      for (String val : values)
      {
        if (val.length() > 0)
        { m_metadata.add(key, val);
        }
      }
    }
  }
  
  /**
   * Removes all values associated with the given key.
   */
  public void remove(String key)
  {
    m_metadata.remove(key);
  }
  
  public void clear()
  {
    m_metadata.clear();
  }
  
  /**
   * @return all values associated with the given key, or null if none.
   */
  public String[] get(String key)
  {
    String[] vals = m_metadata.getValues(key);
    return (vals == null || vals.length == 0) ? null : vals;
  }

   /**
   * @return the first value associated with the given key, or null if none.
   */
  public String getFirst(String key)
  {
    String[] vals = m_metadata.getValues(key);
    return (vals == null || vals.length == 0) ? null : vals[0];
  }
  
  public void setAll(Metadata nutchMeta)
  {    
    m_metadata.clear();
    
    String[] keys = nutchMeta.names();
    String[] vals;
      
    for (int i = 0; i < keys.length; i++)
    {
      vals = nutchMeta.getValues(keys[i]);

      for (int j = 0; j < vals.length; j++)
      { m_metadata.add(keys[i], vals[j]);
      }
    }
  }
  
  public boolean hasKey(String key)
  {
    return (m_metadata.get(key) != null);
  }
  
  public int numKeys()
  {
    return m_metadata.size();
  }
  
  public String[] keys()
  {
    return m_metadata.names();
  }

  public String toString()
  {
    StringBuilder strBld = new StringBuilder();
    
    strBld.append((m_typeLabel.length() > 0) ? m_typeLabel + " " : "");
    strBld.append("MetaData : ");
    
    strBld.append(m_metadata.toString());
    
    return strBld.toString();
  }
  
  public boolean equals(Object obj)
  {    
    boolean same = obj instanceof MetaData;
    
    if (same)
    {
      MetaData other = (MetaData)obj;
      same = m_typeLabel.equals(other.m_typeLabel) && m_metadata.equals(other.m_metadata);
    }
    
    return same;
  }

  /**
   * Persist the object's state.
   */
  public void persist(XMLStreamWriter writer) throws XMLStreamException
  {   
    writer.writeStartElement(XML_TAG_METADATA);

    if (m_typeLabel.length() > 0)
    { writer.writeAttribute(XML_ATTRIB_TYPE, m_typeLabel.toString());
    }
    
    String[] names = m_metadata.names();
    String[] vals;

    for (String name : names)
    {
      vals = m_metadata.getValues(name);
      
      for (String val : vals)
      {
        writer.writeStartElement(name);
        writer.writeCharacters(val);
        writer.writeEndElement();
      }
    }

    writer.writeEndElement();
  }
  
  public void unpersist(XMLStreamReader reader) throws XMLStreamException
  {
    String elemTag;
    String elemVal;
    
    m_typeLabel = reader.getAttributeValue(0);
    m_metadata.clear();
    
    while (true)
    {
      int event = reader.next();
      if (event == XMLStreamConstants.END_ELEMENT && XML_TAG_METADATA.equals(reader.getName().toString()))
      {
         break;
      }

      if (event == XMLStreamConstants.START_ELEMENT)
      {
        elemTag = reader.getName().toString();
        elemVal = reader.getElementText();
        
        add(elemTag, elemVal);
      }
    }
  }
  
  public void readFields(DataInput in) throws IOException
  {
    m_typeLabel = Text.readString(in);
    
    m_metadata.clear();
    m_metadata.readFields(in);
  }

  public void write(DataOutput out) throws IOException
  {
    Text.writeString(out, m_typeLabel);
    m_metadata.write(out);
  }
  
  protected String m_typeLabel;
  protected Metadata m_metadata;
}
