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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import babel.prep.extract.NutchChunk;

import babel.util.language.Language;
import babel.util.persistence.XMLPersistable;

public class Page implements XMLPersistable, Writable
{
  public static final Log LOG = LogFactory.getLog(Page.class);
  
  private static final String XML_TAG_PAGE = "Page";
  private static final String XML_ATTRIB_URL = "URL";

  private static final String PROP_LANG = "Language";
  
  public Page()
  {
    this(null);
  }
  
  public Page(String url)
  {    
    m_pageProps = new MetaData("PageProperties");
    m_pageURL = (url == null) ? new String() : url;
    m_versions = new ArrayList<PageVersion>();
  }
  
  public Page(String url, Iterator<NutchChunk> values)
  {
    HashMap<String, List<NutchChunk>> verChunks = splitIntoVersions(values);

    m_pageProps = new MetaData("PageProperties");
    m_pageURL = (url == null) ? new String() : url; // TODO: Is URL already normalized?
    m_versions = new ArrayList<PageVersion>(verChunks.size());
    
    PageVersion curVer;
    
    for (String segId : verChunks.keySet())
    {
      curVer = new PageVersion(segId, verChunks.get(segId), this);
      
      if (curVer.isNutchComplete())
      { addVersion(curVer);
      }
    }
    
    m_versions.trimToSize();
  }
  
  /**
   * Adds page properties and versions from the given page. If a page property
   * already exists, the values are ignored. If a version already exists, keeps 
   * whichever was fetched earlier.
   */
  public void merge(Page other)
  {
    if (other == null || !m_pageURL.equals(other.m_pageURL))
    { throw new IllegalArgumentException("null ref or different URL");
    }
    
    addProperties(other.m_pageProps);
    
    for (PageVersion ver : other.m_versions)
    { 
      addVersion(ver);
    }
  }
  
  public String pageURL()
  {
    return m_pageURL;
  }
  
  /**
   * @return page properties
   */
  public MetaData pageProperties()
  {
    return m_pageProps;
  }
  
  public Language getLanguage()
  {
    return Language.fromString(m_pageProps.getFirst(PROP_LANG));
  }

  public void setLanguage(Language lang)
  {
    String oldLang = m_pageProps.getFirst(PROP_LANG);
    String newLang = (lang != null) ? lang.toString() : null;
    
    m_pageProps.remove(PROP_LANG);
    
    if (newLang != null)
    { m_pageProps.set(PROP_LANG, newLang);
    }
    
    if (LOG.isWarnEnabled() && (oldLang != null) && !oldLang.equals(newLang))
    { LOG.warn("Changing language of " + m_pageURL + " from " + oldLang + " to " + (newLang == null ? " nothing." : newLang + "."));
    }
  }
  
  /**
   * Adds page properties. If a key is already containined, new values are 
   * ignored.
   */
  public void addProperties(MetaData props)
  {
    if (props != null)
    {
      String[] keys = props.keys();
      
      for (int i = 0; i < keys.length; i++)
      {
        if (!m_pageProps.hasKey(keys[i]))
        {
          m_pageProps.add(keys[i], props.get(keys[i]));
        }
      }
    }
  }
  
  public int numVersions()
  {
    return (m_versions == null) ? 0 : m_versions.size();
  }
  
  /**
   * @return page versions or null if none
   */
  public List<PageVersion> pageVersions()
  {
    return m_versions;
  }
  
  /**
   * Adds a page version to a page. If same version already exists, keeps
   * whichever was fetched earlier.
   */
  public boolean addVersion(PageVersion ver)
  {
    boolean added = false;
    int idx;
    Long fetchCur, fetchOther;
    
    // If same page exists - keep the version that was fetched earlier
    if ((idx = m_versions.indexOf(ver)) >= 0)
    {
      fetchCur = m_versions.get(idx).getFetchTime();
      fetchOther = ver.getFetchTime();
      
      if (fetchCur != null && fetchOther != null && fetchCur > fetchOther)
      {
        m_versions.remove(idx);
        added = m_versions.add(ver);
      }
    }
    else
    {
      added = m_versions.add(ver);          
    }
    
    return added;
  }
  
  public String toString()
  {
    StringBuilder strBld = new StringBuilder();
    
    strBld.append("Page URL: " + m_pageURL + "\n\n");
        
    for (PageVersion ver : m_versions)
    { strBld.append(ver.toString() + "\n");
    }

    return strBld.toString();
  }
  
  public void persist(XMLStreamWriter writer) throws XMLStreamException
  {
    writer.writeStartElement(XML_TAG_PAGE);
    writer.writeAttribute(XML_ATTRIB_URL, m_pageURL);
    
    if (m_pageProps.numKeys() > 0)
    { m_pageProps.persist(writer);
    }

    for (PageVersion ver : m_versions)
    { ver.persist(writer);
    }
    
    writer.writeEndElement();
  }
  

  public void unpersist(XMLStreamReader reader) throws XMLStreamException
  {
    String elemTag;
    PageVersion ver;
    
    m_pageURL = reader.getAttributeValue(0);
    m_versions.clear();
    
    while (true)
    {
      int event = reader.next();
      
      if (event == XMLStreamConstants.END_ELEMENT && XML_TAG_PAGE.equals(reader.getName().toString()))
      { break;
      }

      if (event == XMLStreamConstants.START_ELEMENT)
      {
        elemTag = reader.getName().toString();
        
        if ("MetaData".equals(elemTag))
        {
          m_pageProps.unpersist(reader);
        }
        else if ("PageVersion".equals(elemTag))
        {
          ver = new PageVersion();
          ver.unpersist(reader);
          
          m_versions.add(ver);
        }
      }
    }
  }
  
  public void readFields(DataInput in) throws IOException
  {
    m_pageURL = Text.readString(in);
    m_pageProps.readFields(in);
    
    int numVersions = WritableUtils.readVInt(in);
    m_versions = new ArrayList<PageVersion>(numVersions);
    
    PageVersion curVer;

    for (int i = 0; i < numVersions; i++)
    {
      curVer = new PageVersion();
      curVer.readFields(in);
      m_versions.add(curVer);
    }
  }

  public void write(DataOutput out) throws IOException
  {
    Text.writeString(out, m_pageURL);
    m_pageProps.write(out);
    
    WritableUtils.writeVInt(out, m_versions.size());

    for (PageVersion ver : m_versions)
    { ver.write(out);
    }
  }
  
  protected HashMap<String, List<NutchChunk>> splitIntoVersions(Iterator<NutchChunk> values)
  {
    HashMap<String, List<NutchChunk>> verChunks = new HashMap<String, List<NutchChunk>>();
    
    if (values != null)
    {
      String curSegId;
      NutchChunk curChunk;
      List<NutchChunk> curList;
    
      while (values.hasNext())
      {
        curChunk = new NutchChunk(values.next());
        curSegId = curChunk.getSegmentId();
      
        if (null == (curList = verChunks.get(curSegId)))
        { verChunks.put(curSegId, curList = new LinkedList<NutchChunk>());
        }
      
        curList.add(curChunk);
      }
    }
    
    return verChunks;
  }
  
  protected MetaData m_pageProps;
  protected String m_pageURL;
  protected ArrayList<PageVersion> m_versions;
}
