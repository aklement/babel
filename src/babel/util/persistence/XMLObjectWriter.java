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

package babel.util.persistence;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamWriter;


/**
 * Helper class for serializing Persistable objects using StAX.
 */
public class XMLObjectWriter
{
  protected static final String ENCODING = "utf-8";
  protected static final String ROOT_TAG = "Root";

  public XMLObjectWriter(OutputStream stream, boolean addHeader) throws XMLStreamException, FactoryConfigurationError
  {
    m_addHeader = addHeader;
    m_outStream = stream;
    m_xmlWriter = XMLOutputFactory.newInstance().createXMLStreamWriter(stream, ENCODING);
    
    if (m_addHeader)
    {
      m_xmlWriter.writeStartDocument(ENCODING, "1.0");
      m_xmlWriter.writeStartElement(ROOT_TAG);
    }    
  }
  
  public XMLObjectWriter(String fileName, boolean addHeader) throws IOException, XMLStreamException
  {
    this(new FileOutputStream(fileName), addHeader);
  }
  
  public void write(XMLPersistable obj) throws XMLStreamException
  {
    obj.persist(m_xmlWriter);
  }
  
  public void close() throws XMLStreamException, IOException
  {
    if (m_addHeader)
    {
      m_xmlWriter.writeEndElement();
      m_xmlWriter.writeEndDocument();
    }
      
    // XMLStreamWriter does not close the stream, so must close explicitly
    m_outStream.close();
    m_xmlWriter.close();  
  }
  
  public static void write(String fileName, XMLPersistable obj, boolean addHeader) throws IOException, XMLStreamException
  {
    XMLObjectWriter writer = new XMLObjectWriter(fileName, addHeader);
    writer.write(obj);
    writer.close();
  }

  public static void write(String fileName, List<XMLPersistable> objs, boolean addHeader) throws IOException, XMLStreamException
  {
    XMLObjectWriter writer = new XMLObjectWriter(fileName, addHeader);
      
    for (XMLPersistable obj : objs)
    { writer.write(obj);
    }
      
    writer.close();
  }

  protected OutputStream m_outStream;
  protected XMLStreamWriter m_xmlWriter;
  protected boolean m_addHeader;
}
