package main;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
//import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.xml.stream.XMLStreamException;

//import org.apache.commons.configuration.XMLConfiguration;

//import babel.content.corpora.accessors.LexCorpusAccessorOld;
//import babel.content.eqclasses.EquivalenceClass;
//import babel.content.eqclasses.SimpleEquivalenceClass;
//import babel.content.eqclasses.collectors.SimpleEquivalenceClassCollector;
//import babel.content.eqclasses.properties.NumberContextCollector;
import babel.content.corpora.accessors.CrawlCorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.content.pages.Page;
import babel.content.pages.PageVersion;
//import babel.util.config.Configurator;
import babel.util.persistence.XMPPageReader;

//import babel.content.eqclasses.properties.Context;

public class Test
{

  public static void main(String[] args) throws Exception
  {
    String path = "/Users/aklement/Resources/TestCrawls/Tiny/datedcorpus/datedcorpus.100514-115748/en";

    SimpleDateFormat sdf = new SimpleDateFormat( "yy-MM-dd" );
    Date fromDate = sdf.parse("08-12-29");
    Date toDate = sdf.parse("09-01-05");
    
    CrawlCorpusAccessor a = new CrawlCorpusAccessor(path, fromDate, toDate);
    BufferedReader r = new BufferedReader(a.getCorpusReader());
    String s;
    
    //while (null != (s = r.readLine()))
    //{
    //  System.out.println(s);
    //}
    
    
    while (a.nextDay())
    {
      BufferedReader reader = new BufferedReader(a.getCurDayReader());
      String curLine;

      System.out.println(a.getCurDay() + " -->");
      
      while ((curLine = reader.readLine()) != null)
      {
       System.out.println(curLine);
      }
        
      reader.close();
    }
    
    
    
    
    
    
    /*
    XMLConfiguration config = new XMLConfiguration();
    config.addProperty("test", new Boolean(true));

    config.save("test.xml");
    
    config.load("test.xml");
    
    boolean one = config.getBoolean("test");
    
    System.out.println(one);
    //readPages(args[0]);

    LexCorpusAccessorOld a = new LexCorpusAccessorOld(".*\\.en");

    SimpleEquivalenceClassCollector c = new SimpleEquivalenceClassCollector(SimpleEquivalenceClass.class.getName(), false);
    
    int num = c.collect(a.getCorpusReader(), -1);
    
    NumberContextCollector n = new NumberContextCollector(SimpleEquivalenceClass.class.getName(), false, 2, 2, c.getEquivalenceClass());

    n.collectProperty(a, c.getEquivalenceClass());
    
    List<EquivalenceClass> eqs = c.getEquivalenceClass();
    
    for (EquivalenceClass e : eqs)
    {
      ((Context)(e.getProperty("babel.content.eqclasses.properties.Context"))).pruneContext(1);
    }
    
    int i = 0;
    */
    
  }
  
  static void readPages(String fileName) throws FileNotFoundException, XMLStreamException
  {
    XMPPageReader reader = new XMPPageReader();
    
    List<Page> pages = reader.readPages(fileName);
    
    for (Page p : pages)
    {
      for (PageVersion v : p.pageVersions())
      {
        System.out.print(v.getModificationTime());
      }
      System.out.println(p.toString());
    }
  }
}
