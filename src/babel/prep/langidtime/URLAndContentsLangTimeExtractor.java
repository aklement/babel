package babel.prep.langidtime;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import babel.content.pages.Page;
import babel.content.pages.PageVersion;
import babel.util.language.GoogleLangDetector;
import babel.util.language.LangDetectionResult;
import babel.util.language.LangDetector;
import babel.util.language.Language;

import com.ibm.icu.text.DateFormat;
import com.ibm.icu.text.SimpleDateFormat;

public class URLAndContentsLangTimeExtractor
{
  protected static Pattern DW_CONT_DATE_1 = Pattern.compile("Deutsche Welle \\| (\\d?\\d.\\d?\\d.\\d{4})"); //Deutsche Welle | 12.07.2010
  protected static DateFormat DW_CONT_DATEFORMAT_1 = new SimpleDateFormat("d.M.y");

  protected static DateFormat BBC_URL_DATEFORMAT = new SimpleDateFormat("yyMMdd");
  
  protected static Pattern BBC_EN_CONT_DATE_1 = Pattern.compile("\\d\\d:\\d\\d\\s[^\\s\\d]{3},\\s[^\\s\\d]+,\\s\\d?\\d\\s[^\\s\\d]+\\s\\d\\d\\d\\d"); // 09:30 GMT, Thursday, 19 November 2009
  protected static DateFormat BBC_EN_CONT_DATEFORMAT_1 = new SimpleDateFormat("HH:mm zzz, E, d MMM y");

  protected static Pattern BBC_EN_CONT_DATE_2 = Pattern.compile("[^\\s\\d]+,\\s(\\d?\\d\\s[^\\s\\d]+,\\s\\d\\d\\d\\d,\\s\\d?\\d:\\d\\d\\s[^\\s\\d]{3})"); // Thursday, 11 November, 2004, 00:28 GMT
  protected static DateFormat BBC_EN_CONT_DATEFORMAT_2 = new SimpleDateFormat("d MMM, y, HH:mm zzz");

  protected static Pattern BBC_EN_CONT_DATE_3 = Pattern.compile("[^\\s\\d]+,\\s(\\d?\\d\\s[^\\s\\d]+\\s\\d\\d\\d\\d,\\s\\d?\\d:\\d\\d\\s[^\\s\\d]{3})"); // Saturday, 19 January 2008, 19:10 GMT 
  protected static DateFormat BBC_EN_CONT_DATEFORMAT_3 = new SimpleDateFormat("d MMM y, HH:mm zzz");

  protected static Pattern BBC_RU_CONT_DATE_1 = Pattern.compile("[^\\s\\d]+,\\s(\\d?\\d\\s[^\\s\\d]+\\s\\d\\d\\d\\d\\sг.,\\s\\d?\\d:\\d\\d\\s[^\\s\\d]{3})"); // "воскресенье, 2 августа 2009 г., 15:57 GMT
  protected static DateFormat BBC_RU_CONT_DATEFORMAT_1 = new SimpleDateFormat("d MMM y г., HH:mm zzz", new Locale("ru"));

  protected static Pattern BBC_AZ_CONT_DATE_1 = Pattern.compile("[\\D]+(\\d?\\d\\s[^\\s\\d]+,\\s\\d\\d\\d\\d)\\s-\\sPublished(\\s\\d?\\d:\\d\\d\\s[^\\s\\d]{3})"); // 11 Dekabr, 2008 - Published 16:28 GMT
  protected static DateFormat BBC_AZ_CONT_DATEFORMAT_1 = new SimpleDateFormat("d MMM, y HH:mm zzz", new Locale("az"));

  protected static Pattern BBC_SQ_CONT_DATE_1 = Pattern.compile("[\\D]+,\\s(\\d?\\d\\s[^\\s\\d]+\\s\\d\\d\\d\\d\\s-\\s\\d?\\d:\\d\\d\\s[^\\s\\d]{3})"); // 11 Dekabr, 2008 - Published 16:28 GMT
  protected static DateFormat BBC_SQ_CONT_DATEFORMAT_1 = new SimpleDateFormat("d MMM y - HH:mm zzz", new Locale("sq"));
  
  protected static Pattern BBC_CY_CONT_DATE_1 = Pattern.compile("[^\\s\\d]+,\\s(\\d?\\d\\s[^\\s\\d]+,\\s\\d\\d\\d\\d,\\s\\d?\\d:\\d\\d\\s[^\\s\\d]{3})");  // Dydd Gwener, 5 Mawrth, 2004, 20:35 GMT
  protected static DateFormat BBC_CY_CONT_DATEFORMAT_1 = new SimpleDateFormat("d MMM, y, HH:mm zzz", new Locale("cy"));
  
  protected static Pattern BBC_ES_CONT_DATE_1 = Pattern.compile("[^\\s\\d]+,\\s(\\d?\\d\\s[\\D]+\\s\\d{4})\\s+-\\s+(\\d?\\d:\\d\\d\\s[^\\s\\d]{3})");  // Miércoles, 23 de abril de 2008
  protected static DateFormat BBC_ES_CONT_DATEFORMAT_1 = DateFormat.getDateInstance(DateFormat.LONG, new Locale("es"));
  
  protected static Pattern BBC_BN_CONT_DATE_1 = Pattern.compile("[\\D]*(\\d?\\d\\s[^\\s\\d]+\\s\\d\\d\\d\\d\\s-\\s\\d?\\d:\\d\\d)");
  protected static DateFormat BBC_BN_CONT_DATEFORMAT_1 = new SimpleDateFormat("d MMM y - HH:mm", new Locale("bn"));

  protected static Pattern BBC_ZH_CONT_DATE_1 = Pattern.compile("(\\d{4})年\\s*(\\d?\\d)月\\s*(\\d?\\d)日"); //2009年03月21日 | 2010年 5月 09日
  protected static DateFormat BBC_ZH_CONT_DATEFORMAT_1 = new SimpleDateFormat("y M d");
  
  //protected static Pattern BBC_UR_CONT_DATE_1 = Pattern.compile("(\\d{2})\\s*([^\\s\\d]+)\\s*(\\d{4})\\s*,‭\\s*(\\d?\\d:\\d\\d\\s[^\\s\\d]{3})");
  //protected static DateFormat BBC_UR_CONT_DATEFORMAT_1 = new SimpleDateFormat("d m y HH:mm zzz", new Locale("ur"));

  protected static Pattern VOA_URL_DATE_ONE = Pattern.compile("\\d\\d\\d\\d_\\d?\\d_\\d?\\d");
  protected static DateFormat VOA_URL_DATEFORMAT_ONE = new SimpleDateFormat("yyyy_M_d");

  protected static Pattern VOA_URL_DATE_TWO = Pattern.compile("\\d\\d\\d\\d-\\d?\\d-\\d?\\d");
  protected static DateFormat VOA_URL_DATEFORMAT_TWO = new SimpleDateFormat("yyyy-M-d");

  protected static Pattern VOA_URL_DATE_THREE = Pattern.compile("\\d?\\d-\\d?\\d-\\d\\d\\d\\d");
  protected static DateFormat VOA_URL_DATEFORMAT_THREE = new SimpleDateFormat("M-d-yyyy");

  protected static Pattern VOA_URL_DATE_FOUR = Pattern.compile("\\d\\d\\D\\D\\D\\d\\d");
  protected static DateFormat VOA_URL_DATEFORMAT_FOUR = new SimpleDateFormat("ddMMMyy");

  protected static Pattern VOA_FA_CONT_DATE_1 = Pattern.compile("(\\d?\\d.\\d?\\d.\\d\\d)"); //9.5.10
  protected static DateFormat VOA_FA_CONT_DATEFORMAT_1 = new SimpleDateFormat("d.M.yy");

  protected static Pattern VOA_SO_CONT_DATE_1 = Pattern.compile("\\D+(\\d?\\d\\s+[\\D\\S]+\\s+\\d{4})"); //06 May 2010
  protected static DateFormat VOA_SO_CONT_DATEFORMAT_1 = new SimpleDateFormat("d MMM yy");
  
  protected static Pattern LENTA_URL_DATE = Pattern.compile("\\d\\d\\d\\d/\\d\\d/\\d\\d");
  protected static DateFormat LENTA_URL_DATEFORMAT = new SimpleDateFormat("yyyy/MM/dd");
 
  protected static Pattern ATLAS_DATE = Pattern.compile("(\\d?\\d\\.\\s*[^\\s\\d]+\\s\\d{4})\\D(\\d?\\d:\\d\\d)"); // 16. novembra 2009 13:00 6. j?la 2007?15:30
  protected static DateFormat ATLAS_DATEFORMAT = new SimpleDateFormat("d. MMM yyyy HH:mm",  new Locale("sk"));
  
  protected static Pattern INFOKAZ_DATE = Pattern.compile("\\d?\\d\\.\\d?\\d.\\d{4}\\s/\\s\\d?\\d:\\d\\d"); // 08.04.2010 / 13:58
  protected static DateFormat INFOKAZ_DATEFORMAT = new SimpleDateFormat("d.M.yyyy / HH:mm");

  protected static Pattern DIENA_DATE = Pattern.compile(",\\s+(\\d?\\d\\.\\s[^\\s\\d]+\\s\\(\\d{4}\\) \\d?\\d:\\d\\d)"); // Svētdiena, 9. maijs (2010) 15:36 
  protected static DateFormat DIENA_DATEFORMAT = new SimpleDateFormat("d. MMM (yyyy) HH:mm",  new Locale("lv"));

  protected static Pattern AZATLIQ_DATE = Pattern.compile("\\d?\\d\\.\\d?\\d.\\d{4}"); //24.12.2009
  protected static DateFormat AZATLIQ_DATEFORMAT = new SimpleDateFormat("d.M.yyyy ");
  
  /*
  Test:

  -BBC-
  
  English :    http://news.bbc.co.uk/1/hi/
               http://news.bbc.co.uk/1/hi/business/7870409.stm  x
               http://news.bbc.co.uk/1/hi/business/7874565.stm  x 12:05 GMT, Friday, 6 February 2009
               http://news.bbc.co.uk/1/hi/uk/7197555.stm        x Saturday, 19 January 2008, 19:10 GMT
               http://news.bbc.co.uk/1/low/business/4001509.stm x Thursday, 11 November, 2004, 00:28 GMT
               http://news.bbc.co.uk/1/shared/bsp/hi/education/07/school_tables/secondary_schools/html/302_5405.stm x Thursday, 10 January, 2008, 00:01 GMT
               http://news.bbc.co.uk/2/hi/africa/1495725.stm -- looks like old pages x Friday, 17 August, 2001, 10:14 GMT 
               http://news.bbc.co.uk/2/hi/asia-pacific/3033409.stm x Friday, 16 May, 2003, 10:57 GMT 
            
  Russian :    http://www.bbc.co.uk/russian/science/2009/08/090802_nissan_electric_car.shtml
               http://news.bbc.co.uk/hi/russian/international/newsid_7820000/7820477.stm
            
  Azeri :      http://www.bbc.co.uk/azeri/news/story/2008/12/081211_impartiality.shtml
  
  Albanian :   http://www.bbc.co.uk/albanian/news/2009/11/091105_kosovo_elections_info.shtml x Botuar: E enjte, 05 nëntor 2009 - 15:04 CET
  
  Chinese:     http://news.bbc.co.uk/chinese/simp/hi/newsid_3030000/newsid_3030900/3030959.stm
  
  -VoA-
  
  Chinese:    http://www1.voanews.com/chinese/news/disaster/POLISH-PRESIDENT-20100410-90537044.html
  
  Persian:    http://www1.voanews.com/persian/news/a-31-2008-11-04-voa9-62746067.html 
  
 
 -Atlas-
 
 Slovak:      http://dnes.atlas.sk/ekonomika/157041/billa-zavrie-v-cesku-devat-obchodov
 
 -Lenta-
 
 Russian:    http://www.lenta.ru/articles/2008/04/30/abkhaz/
 
 -Diena-
 
 Latvian:    http://www.diena.lv/lat/business/hotnews/izdevnieciba-zurnals-santa-ievies-savu-zurnalu-digitalas-versijas
 
 -Azatliq-
 
 Kyrgyz:     http://www.azatliq.org/archive/around_the_world/20090715/577/584.html
  */

  // Slovak: dnes.atlas.sk - from page version content using a date pattern
  // English: bbc.
  
  public URLAndContentsLangTimeExtractor(String referrer)
  {
    m_bbcURLLangs = new HashMap<String, LangDetectionResult>();
            
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?azeri/.*", new LangDetectionResult(Language.AZERBAIJANI)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?albanian/.*", new LangDetectionResult(Language.ALBANIAN)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?bengali/.*", new LangDetectionResult(Language.BENGALI)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?persian/.*", new LangDetectionResult(Language.PERSIAN));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?hindi/.*", new LangDetectionResult(Language.HINDI));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?arabic/.*", new LangDetectionResult(Language.ARABIC));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?indonesia/.*", new LangDetectionResult(Language.INDONESIAN));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?irish/.*", new LangDetectionResult(Language.IRISH));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?kyrgyz/.*", new LangDetectionResult(Language.KYRGYZ));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?nepali/.*", new LangDetectionResult(Language.NEPALI));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?russian/.*", new LangDetectionResult(Language.RUSSIAN)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?pashto/.*", new LangDetectionResult(Language.PASHTO));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?serbian/.*", new LangDetectionResult(Language.SERBIAN));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?somali/.*", new LangDetectionResult(Language.SOMALI));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?tamil/.*", new LangDetectionResult(Language.TAMIL));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?turkce/.*", new LangDetectionResult(Language.TURKISH));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?turkish/.*", new LangDetectionResult(Language.TURKISH));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?ukrainian/.*", new LangDetectionResult(Language.UKRANIAN));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?urdu/.*", new LangDetectionResult(Language.URDU));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?uzbek/.*", new LangDetectionResult(Language.UZBEK));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?cymru/.*", new LangDetectionResult(Language.WELSH));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?welsh/.*", new LangDetectionResult(Language.WELSH)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?mundo/.*", new LangDetectionResult(Language.SPANISH));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?spanish/.*", new LangDetectionResult(Language.SPANISH));
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?chinese/simp/.*", new LangDetectionResult(Language.CHINESE_SIMPLIFIED)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?chinese/trad/.*", new LangDetectionResult(Language.CHINESE_TRADITIONAL)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?ukchina/simp/.*", new LangDetectionResult(Language.CHINESE_SIMPLIFIED)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?ukchina/trad/.*", new LangDetectionResult(Language.CHINESE_TRADITIONAL)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?zhongwen/simp/.*", new LangDetectionResult(Language.CHINESE_SIMPLIFIED)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?zhongwen/trad/.*", new LangDetectionResult(Language.CHINESE_TRADITIONAL)); //
    m_bbcURLLangs.put("^bbc.co.uk/(hi/|low/)?english/.*", new LangDetectionResult(Language.ENGLISH)); //
    m_bbcURLLangs.put("^bbc.co.uk/local/.*", new LangDetectionResult(Language.ENGLISH)); //
    m_bbcURLLangs.put("^bbc.co.uk/[12]/.*", new LangDetectionResult(Language.ENGLISH)); //
    
    m_voaURLLangs = new HashMap<String, LangDetectionResult>();

    m_voaURLLangs.put("^voanews.com/armenian/.*", new LangDetectionResult(Language.ARMENIAN));
    m_voaURLLangs.put("^voanews.com/bosnian/.*", new LangDetectionResult(Language.BOSNIAN));
    m_voaURLLangs.put("^voanews.com/azerbaijani/.*", new LangDetectionResult(Language.AZERBAIJANI));
    m_voaURLLangs.put("^voanews.com/indonesian/.*", new LangDetectionResult(Language.INDONESIAN));
    m_voaURLLangs.put("^voanews.com/pashto/.*", new LangDetectionResult(Language.PASHTO));
    m_voaURLLangs.put("^voanews.com/persian/.*", new LangDetectionResult(Language.PERSIAN)); //
    m_voaURLLangs.put("^voanews.com/russian/.*", new LangDetectionResult(Language.RUSSIAN));
    m_voaURLLangs.put("^voanews.com/somali/.*", new LangDetectionResult(Language.SOMALI)); //
    m_voaURLLangs.put("^voanews.com/swahili/.*", new LangDetectionResult(Language.SWAHILI));
    m_voaURLLangs.put("^voanews.com/thai/.*", new LangDetectionResult(Language.THAI));
    m_voaURLLangs.put("^voanews.com/turkish/.*", new LangDetectionResult(Language.TURKISH));
    m_voaURLLangs.put("^voanews.com/ukrainian/.*", new LangDetectionResult(Language.UKRANIAN));
    m_voaURLLangs.put("^voanews.com/urdu/.*", new LangDetectionResult(Language.URDU));
    m_voaURLLangs.put("^voanews.com/uzbek/.*", new LangDetectionResult(Language.UZBEK)); 
    m_voaURLLangs.put("^voanews.com/chinese/.*", new LangDetectionResult(Language.CHINESE)); //
    
    m_detector = new GoogleLangDetector(referrer);
  }
  
  public static void main(String[] str) throws Exception
  {
    /*
    String[] arr = 
    {"http://www.voanews.com/armenian/2010-04-28-voa3.cfm",
     "http://www1.voanews.com/azerbaijani/news/usa/Goldman-Hearinf-04-27-2010-92207809.html",
     "http://www1.voanews.com/persian/news/asia/afgh-anniversary-2010-04-28-92307209.html",
     "http://www.voanews.com/pashto/2010-04-22-voa8.cfm",
     "http://www1.voanews.com/russian/news/former-ussr/Uzbek-delegation_2010_04_28-92316159.html",
     "http://www1.voanews.com/urdu/news/world/Thailand-Demonstrations-27Apr09-92201884.html",
     "http://www.bbc.co.uk/azeri/news/story/2010/04/100428_eynulla.shtml",
     "http://www.bbc.co.uk/russian/international/2010/04/100428_greece_default_denial.shtml",
     "http://lenta.ru/news/2010/04/28/tim/"};
     */
    
    URLAndContentsLangTimeExtractor d = new URLAndContentsLangTimeExtractor("http://www.clsp.jhu.edu");
    Date date = d.detectDWDateFromContent(readFileAsString("test/0,,5787445,00.html"));
    
    System.out.println(date);
    
    /*
    date = d.detectBBCDateFromEnContent(";Wednesday, 7 May 2003, 15:56 GMT 16:56 UKjk ");
    date = d.detectBBCDateFromEnContent(" fgjh>Thursday, 1 November, 2004, 00:28 GMT< dhdish ");
    date = d.detectBBCDateFromEnContent("lkj12:05 GMT, Friday, 6 February 2009 jhkjh");
    date = d.detectBBCDateFromEnContent("kThursday, 10 January, 2008, 00:01 GMT");
    date = d.detectBBCDateFromEnContent("jkh Saturday, 19 January 2008, 19:10 GMTdkjfhkj ");
    date = d.detectBBCDateFromRuContent(" kj воскресенье, 9 мая 2010 г., 01:05 GMT 05:05 MCK<");
    date = d.detectBBCDateFromRuContent("бновлено: </span>пятница, 09 января 2009 г., 14:34 GMT 17:34 MCK</spa");
    date = d.detectBBCDateFromAzContent("б 08 May, 2010 - Published 10:10 GMT a");
    date = d.detectBBCDateFromSqContent(" Botuar: E enjte, 05 nëntor 2009 - 15:04 CETfgg");
    date = d.detectBBCDateFromBnContent(" B8 May 2010 - 16:18");
    date = d.detectBBCDateFromCyContent("Dydd Gwener, 5 Mawrth, 2004, 20:35 GMT");
    date = d.detectBBCDateFromEsContent("Miércoles, 23 de abril de 2008 - 21:37 GMT");
    date = d.detectBBCDateFromZhContent(">2009年03月21日 格林");
    date = d.detectVoADateFromFaContent(">8.05.10a ");
    date = d.detectVoADateFromSoContent("jhs 06 May 2010");
    */
  }
  
  private static String readFileAsString(String path) throws Exception
  {
    StringBuilder fileData = new StringBuilder();
    BufferedReader reader = new BufferedReader(new FileReader(path));
    char[] buf = new char[1024];
    int numRead = 0;
    
    while((numRead = reader.read(buf)) != -1)
    {
      String readData = String.valueOf(buf, 0, numRead);
      fileData.append(readData);
    }
    
    reader.close();
    return fileData.toString();
  }
  
  public DetectionResult detect(Page page)
  {
    DetectionResult result = null;
    
    if (page != null)
    {      
      try
      {
        if (null != (result = detectBBC(page)));
        else if (null != (result = detectVoA(page)));
        else if (null != (result = detectDW(page)));
        else if (null != (result = detectLenta(page)));
        else if (null != (result = detectAtlas(page)));        
        else if (null != (result = detectInformKaz(page)));
        else if (null != (result = detectDienaLv(page)));
        else if (null != (result = detectAzatliqKy(page)));
        else if (null != (result = detectOther(page)));
      }
      catch(Exception e)
      { result = null;
      }
    }
    
    return result;
  }
  
  protected DetectionResult detectOther(Page page)
  {
    // A catchall: runs google lang ID on all other pages  
    DetectionResult result = null;

    try
    {
      // Get or detect language
      LangDetectionResult langResult = getOrGoogDetectLang(page);
        
      if (langResult != null)
      { result = new DetectionResult(langResult);
      }
    }
    catch (Exception e)
    {}
    
    return result;
  }
  
  protected DetectionResult detectDW(Page page)
  {
    // Example URLs: http://www.dw-world.de/dw/article/0,,5862718,00.html  
    
    DetectionResult result = null;
    String url = removeProtocol(page.pageURL());
    
    if (url.matches("^(www.)?dw-world.de.*"))
    { 
      try
      {
        // Get or detect language
        LangDetectionResult langResult = getOrGoogDetectLang(page);
        
        if (langResult != null)
        {
          result = new DetectionResult(langResult);
          Date modTime = null;
          
          // Detect date from contents of each pageversion
          for (PageVersion ver : page.pageVersions())
          {
            if (null != (modTime = detectDWDateFromContent(ver.getContent())))
            { result.addModTime(ver, modTime);            
            }
          }
        }
      }
      catch (Exception e)
      {}
    }
    
    return result;
  }
  
  protected LangDetectionResult getOrGoogDetectLang(Page page) throws Exception
  {
    Language lang = page.getLanguage();      
    return (lang == null) ? m_detector.detect(page.pageVersions().get(0).getContent()) : new LangDetectionResult(lang);
  }
  
  protected Date detectDWDateFromContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = DW_CONT_DATE_1.matcher(content);
        
      if (m.find())
      {
        modTime = DW_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }
  
  protected DetectionResult detectBBC(Page page)
  {
    // Example URL: http://www.bbc.co.uk/azeri/news/story/2010/04/100428_eynulla.shtml
    
    DetectionResult result = null;
    String url = removeProtocolAndPrefix(page.pageURL());
    LangDetectionResult langResult = null;
    Date modTime = null;
    
    for (String pref : m_bbcURLLangs.keySet())
    {
      if (url.matches(pref))
      {
        langResult = m_bbcURLLangs.get(pref);
        break;
      }
    }
    
    if (langResult != null)
    {
      result = new DetectionResult(langResult);

      // Try detecting from URL
      if (null == (modTime = detectBBCDateFromURL(url)))
      {
        // If failed, detect from contents of each pageversion
        for (PageVersion ver : page.pageVersions())
        {
          if (null != (modTime = detectBBCDateFromContent(langResult.language(), ver.getContent())))
          {
            result.addModTime(ver, modTime);            
          }
        }
      }
      else
      {
        result.addModTimeAllVers(page, modTime);        
      }
    }
    
    return result;
  }
  
  protected Date detectBBCDateFromURL(String url)
  {
    Date modTime = null;

    try
    {
      int slashIdx = url.lastIndexOf("/");
      int underscoreIdx = url.indexOf("_", slashIdx);      
    
      if (underscoreIdx - slashIdx == 7)
      {
        String dateStamp = url.substring(slashIdx + 1, underscoreIdx); 
        modTime = BBC_URL_DATEFORMAT.parse(dateStamp);
      }
    }
    catch (Exception e)
    {
      modTime = null;
    }
    
    return modTime;
  }

  protected Date detectBBCDateFromContent(Language lang, String content)
  {
    Date modTime = null;

    if (Language.ENGLISH.equals(lang))
    { modTime = detectBBCDateFromEnContent(content);
    }
    else if (Language.RUSSIAN.equals(lang))
    { modTime = detectBBCDateFromRuContent(content);
    }
    else if (Language.AZERBAIJANI.equals(lang))
    { modTime = detectBBCDateFromAzContent(content);
    }    
    else if (Language.ALBANIAN.equals(lang))
    { modTime = detectBBCDateFromSqContent(content);
    }
    else if (Language.BENGALI.equals(lang))
    { modTime = detectBBCDateFromBnContent(content);
    }    
    else if (Language.WELSH.equals(lang))
    { modTime = detectBBCDateFromCyContent(content);
    }
    else if (Language.SPANISH.equals(lang))
    { modTime = detectBBCDateFromEsContent(content);
    }
    else if (Language.CHINESE.equals(lang))
    { modTime = detectBBCDateFromZhContent(content);
    }
    return modTime;
  }
  
  protected Date detectBBCDateFromEnContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_EN_CONT_DATE_1.matcher(content);
        
      if (m.find())
      { modTime = BBC_EN_CONT_DATEFORMAT_1.parse(content.substring(m.start(0), m.end(0)));
      }
    }
    catch (Exception e)
    {}
    
    if (modTime == null)
    {
      try
      {        
        m = BBC_EN_CONT_DATE_2.matcher(content);
          
        if (m.find())
        { modTime = BBC_EN_CONT_DATEFORMAT_2.parse(content.substring(m.start(1), m.end(1)));
        }
      }
      catch (Exception e)
      {}
    }

    if (modTime == null)
    {
      try
      {        
        m = BBC_EN_CONT_DATE_3.matcher(content);
        
        if (m.find())
        { modTime = BBC_EN_CONT_DATEFORMAT_3.parse(content.substring(m.start(1), m.end(1)));
        }
      }
      catch (Exception e)
      {}
    }
    
    return modTime;
  }
  
  protected Date detectBBCDateFromRuContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_RU_CONT_DATE_1.matcher(content);
        
      if (m.find())
      {
        modTime = BBC_RU_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }
  
  protected Date detectBBCDateFromAzContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_AZ_CONT_DATE_1.matcher(content);
        
      if (m.find())
      {
        modTime = BBC_AZ_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)) + content.substring(m.start(2), m.end(2)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }

  protected Date detectBBCDateFromSqContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_SQ_CONT_DATE_1.matcher(content);
        
      if (m.find())
      {
        modTime = BBC_SQ_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }

  protected Date detectBBCDateFromBnContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_BN_CONT_DATE_1.matcher(content);
        
      if (m.find())
      {
        modTime = BBC_BN_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }

  protected Date detectBBCDateFromCyContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_CY_CONT_DATE_1.matcher(content);
      
      if (m.find())
      { 
        modTime = BBC_CY_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }

  protected Date detectBBCDateFromEsContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_ES_CONT_DATE_1.matcher(content);
      
      if (m.find())
      {
        modTime = BBC_ES_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }
  
  protected Date detectBBCDateFromZhContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = BBC_ZH_CONT_DATE_1.matcher(content);
      
      if (m.find())
      {
        modTime = BBC_ZH_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)) + " " + content.substring(m.start(2), m.end(2)) + " " + content.substring(m.start(3), m.end(3)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }
 
  protected DetectionResult detectVoA(Page page)
  { 
    DetectionResult result = null;
    String url = removeProtocolAndPrefix(page.pageURL());
    LangDetectionResult langResult = null;
    Date modTime = null;
    
    for (String pref : m_voaURLLangs.keySet())
    {
      if (url.matches(pref))
      {
        langResult = m_voaURLLangs.get(pref);
        break;
      }
    }
    
    if (langResult != null)
    {
      result = new DetectionResult(langResult);

      // Try detecting from URL
      if (null == (modTime = detectVoADateFromURL(url)))
      {
 
        // If failed, detect from contents of each pageversion
        for (PageVersion ver : page.pageVersions())
        {
          if (null != (modTime = detectVoADateFromContent(langResult.language(), ver.getContent())))
          {
            result.addModTime(ver, modTime);            
          }
        }
      }
      else
      {
        result.addModTimeAllVers(page, modTime);        
      }
    }
    
    return result;
  }
  
  protected Date detectVoADateFromURL(String url)
  {
    // Example URLs: 
    // http://www.voanews.com/armenian/2010-04-28-voa3.cfm
    // http://www1.voanews.com/azerbaijani/news/usa/Goldman-Hearinf-04-27-2010-92207809.html
    // http://www1.voanews.com/persian/news/asia/afgh-anniversary-2010-04-28-92307209.html
    // http://www.voanews.com/pashto/2010-04-22-voa8.cfm
    // http://www1.voanews.com/russian/news/former-ussr/Uzbek-delegation_2010_04_28-92316159.html
    // http://www1.voanews.com/urdu/news/world/Thailand-Demonstrations-27Apr09-92201884.html

    Date modTime = null;
    
    try
    {
      String fileName = url.substring(url.lastIndexOf("/") + 1);
      Matcher m = null;
        
      if (((m = VOA_URL_DATE_ONE.matcher(fileName)) != null) && m.find())
      { modTime = VOA_URL_DATEFORMAT_ONE.parse(m.group());
      }
      else if (((m = VOA_URL_DATE_TWO.matcher(fileName)) != null) && m.find())
      { modTime = VOA_URL_DATEFORMAT_TWO.parse(m.group());
      }
      else if (((m = VOA_URL_DATE_THREE.matcher(fileName)) != null) && m.find())
      { modTime = VOA_URL_DATEFORMAT_THREE.parse(m.group());
      }          
      else if (((m = VOA_URL_DATE_FOUR.matcher(fileName)) != null) && m.find())
      { modTime = VOA_URL_DATEFORMAT_FOUR.parse(m.group());
      }
    }
    catch (Exception e)
    {}
     
    return modTime;    
  }
  
  protected Date detectVoADateFromContent(Language lang, String content)
  {
    Date modTime = null;

    if (Language.CHINESE.equals(lang)) // Same format as on BBC
    { modTime = detectBBCDateFromZhContent(content);
    }
    else if (Language.PASHTO.equals(lang))
    { modTime = detectVoADateFromFaContent(content);
    }
    else if (Language.SOMALI.equals(lang))
    { modTime = detectVoADateFromSoContent(content);
    }
    return modTime;
  }
  
  protected Date detectVoADateFromFaContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = VOA_FA_CONT_DATE_1.matcher(content);
      
      if (m.find())
      {
        modTime = VOA_FA_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }
  
  protected Date detectVoADateFromSoContent(String content)
  {
    Date modTime = null;
    Matcher m = null;
    
    try
    {        
      m = VOA_SO_CONT_DATE_1.matcher(content);
      
      if (m.find())
      {
        modTime = VOA_SO_CONT_DATEFORMAT_1.parse(content.substring(m.start(1), m.end(1)));
      }
    }
    catch (Exception e)
    {}
    
    return modTime;
  }
  
  protected DetectionResult detectLenta(Page page)
  {
    // Example URLs: http://lenta.ru/news/2010/04/28/regime/  
    
    DetectionResult result = null;
    String url = removeProtocol(page.pageURL());
    
    Date modTime = null;
    
    if (url.matches("^(www.)?lenta.ru.*"))
    { 
      try
      {
        Matcher m = LENTA_URL_DATE.matcher(url);
        
        if (m.find())
        { modTime = LENTA_URL_DATEFORMAT.parse(m.group());
        }
      }
      catch (Exception e)
      {}

      result = new DetectionResult(new LangDetectionResult(Language.RUSSIAN));
      result.addModTimeAllVers(page, modTime);
    }
    
    return result;
  }
  
  protected DetectionResult detectAtlas(Page page)
  {
    DetectionResult result = null;
    String url = removeProtocol(page.pageURL());
    
    if (url.startsWith("dnes.atlas.sk"))
    { 
      result = new DetectionResult(new LangDetectionResult(Language.SLOVAK));
      
      String content;
      Matcher m;
      boolean gotit;
      
      for (PageVersion ver : page.pageVersions())
      {
        content = ver.getContent();
        m = ATLAS_DATE.matcher(content);
        gotit = false;

        while (!gotit && m.find())
        {
          try
          { 
            result.addModTime(ver, ATLAS_DATEFORMAT.parse(content.substring(m.start(1), m.end(1)) + " " +  content.substring(m.start(2), m.end(2))));

            gotit = true;
          }
          catch (Exception e)
          {}
        }
      }
    }
    
    return result;
  }
  
  //  http://www.inform.kz/kaz/
  protected DetectionResult detectInformKaz(Page page)
  {
    DetectionResult result = null;
    String url = removeProtocol(page.pageURL());
    
    if (url.matches("^(www.)?inform.kz/kaz/.*"))
    { 
      result = new DetectionResult(new LangDetectionResult(Language.KAZAKH));
      
      String content;
      Matcher m;
        
      for (PageVersion ver : page.pageVersions())
      {
        try
        {
          content = ver.getContent();
          m = INFOKAZ_DATE.matcher(content);
          
          if (m.find())
          {
            result.addModTime(ver, INFOKAZ_DATEFORMAT.parse(content.substring(m.start(0), m.end(0))));
          }
        }
        catch (Exception e)
        {}
      }
    }
    
    return result;
  }

  //  http://www.diena.lv
  protected DetectionResult detectDienaLv(Page page)
  {
    DetectionResult result = null;
    String url = removeProtocol(page.pageURL());
    
    if (url.matches("^(www.)?diena.lv/lat.*"))
    { 
      result = new DetectionResult(new LangDetectionResult(Language.LATVIAN));
      
      String content;
      Matcher m;
        
      for (PageVersion ver : page.pageVersions())
      {
        try
        {
          content = ver.getContent();
          m = DIENA_DATE.matcher(content);
          
          if (m.find())
          {
            result.addModTime(ver, DIENA_DATEFORMAT.parse(content.substring(m.start(1), m.end(1))));
          }
        }
        catch (Exception e)
        {}
      }
    }
    
    return result;
  }
  
  protected DetectionResult detectAzatliqKy(Page page)
  {
    DetectionResult result = null;
    String url = removeProtocol(page.pageURL());
    
    if (url.matches("^(www.)?azatliq.org.*"))
    { 
      result = new DetectionResult(new LangDetectionResult(Language.KYRGYZ));      
      
      String content;
      Matcher m;
        
      for (PageVersion ver : page.pageVersions())
      {
        try
        {
          content = ver.getContent();
          m = AZATLIQ_DATE.matcher(content);
          
          if (m.find())
          {
            result.addModTime(ver, AZATLIQ_DATEFORMAT.parse(content.substring(m.start(0), m.end(0))));
          }
        }
        catch (Exception e)
        {}
      }
    }
    
    return result;
  }
  
  protected String removeProtocolAndPrefix(String url)
  {
    // Strip everything up to first dot, and lowercase
    return url.substring(url.indexOf(".") + 1).toLowerCase();
  }

  protected String removeProtocol(String url)
  {
    // Strip everything up to first dot, and lowercase
    return url.substring(url.indexOf("://") + 3).toLowerCase();
  }
  
  protected HashMap<String, LangDetectionResult> m_bbcURLLangs;
  protected HashMap<String, LangDetectionResult> m_voaURLLangs;
  private LangDetector m_detector;

  public class DetectionResult
  {
    public DetectionResult(LangDetectionResult langDet)
    {
      m_modTimes = new HashMap<PageVersion, Date>();
      m_langDet = langDet;
    }
    
    public void addModTime(PageVersion ver, Date modTime)
    {
      if (modTime != null)
      { m_modTimes.put(ver, modTime);
      }
    }
    
    public void addModTimeAllVers(Page page, Date modTime)
    {
      if (modTime != null)
      {
        for (PageVersion ver : page.pageVersions())
        { m_modTimes.put(ver, modTime);
        }
      }
    }
    
    public String toString()
    {
      StringBuilder strBld = new StringBuilder();
      
      if (m_langDet != null)
      {
        strBld.append("Lang: ");
        strBld.append(m_langDet.language());
        strBld.append(" ");
      }

      if (m_modTimes.size() > 0)
      { strBld.append("Time:");
      }
      
      for (PageVersion ver : m_modTimes.keySet())
      { strBld.append(" " + m_modTimes.get(ver));
      }
        
      return strBld.toString();
    }
    
    HashMap<PageVersion, Date> m_modTimes;
    LangDetectionResult m_langDet;
  }
}

/* Others to encode:
  http://www.dw-world.de/ 
  http://www.eitb.com/eu/
  http://www.gaelport.com/
  
  http://www.apollo.lv/
  http://www.delfi.lv/

  http://www.bernama.com/bernama/v5/bm/
  http://www.maltarightnow.com/
  http://www.montsame.mn/
  http://aktualne.centrum.sk/ 
  http://www.bbc.co.uk/worldservice/languages/index.shtml 
  http://www.france24.com/
*/