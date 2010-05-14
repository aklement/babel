package babel.util.dict;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.json.JSONObject;

import babel.util.language.Language;

// TODO: Finish
public class GoogleDictionaryCollector 
{
  /** Maximum size of the sting sent to google for identification. */
  protected static final String URL_PREF = "http://www.google.com/dictionary/json?callback=dict_api.callbacks.id100&q=";
  protected static final String URL_SUFF = "&restrict=pr%2Cde&client=te";
  protected static final String ENCODING = "UTF-8";
  protected static byte ESCAPE_CHAR = '%';

  /**
   * @param referrer URL of the the organization originating the request.
   */
  public GoogleDictionaryCollector(String referrer)
  {   
    if (referrer == null || referrer.length() == 0)
    { throw new IllegalArgumentException("Referrer must be supplied.");
    }

    m_referrer = referrer;
  }
  
  public void translate(Language from, Language to, String word) throws Exception
  {
    if (word == null || word.length() == 0 || word.contains(" "))
    { throw new IllegalArgumentException();
    }
    
    StringBuilder url = new StringBuilder();
    url.append(URL_PREF);
    url.append(URLEncoder.encode(word, ENCODING));
    url.append("&sl=" + from.toString());
    url.append("&tl=" + to.toString());
    url.append(URL_SUFF);
    
    JSONObject json = retrieveJSON(new URL(url.toString()));
    
    System.out.println(json.toString());
    
   // return new LangDetectionResult(
   //     Language.fromString(json.getJSONObject("responseData").getString("language")), 
   //     json.getJSONObject("responseData").getDouble("confidence"),
   //     json.getJSONObject("responseData").getBoolean("isReliable"));

  }
  
  
  public static void main(String[] args) throws Exception
  {
    GoogleDictionaryCollector collector = new GoogleDictionaryCollector("none");
    
    collector.translate(Language.ENGLISH, Language.SPANISH, "approach");
  }

  /**
   * Forms an HTTP request, sends it using GET method and returns the result of
   * the request as a JSONObject.
   *
   * @param url the URL to query for a JSONObject.
   */
  protected JSONObject retrieveJSON(final URL url) throws Exception
  { 
    try
    {
      final HttpURLConnection uc = (HttpURLConnection) url.openConnection();
      uc.setRequestProperty("referer", m_referrer);
      uc.setRequestMethod("GET");
      uc.setDoOutput(true);
        
      try
      {
        String result = inputStreamToString(uc.getInputStream());
        
        result = result.substring(result.indexOf('{'), result.lastIndexOf('}') + 1);
          
        return new JSONObject(result);
      } 
      finally
      { // http://java.sun.com/j2se/1.5.0/docs/guide/net/http-keepalive.html
        uc.getInputStream().close();
        
        if (uc.getErrorStream() != null)
        { uc.getErrorStream().close();
        }
      }
    }
    catch (Exception ex)
    { throw new Exception("Error retrieving detection result : " + ex.toString(), ex);
    }
  }
    
  /**
   * Reads an InputStream and returns its contents as a String.
   * 
   * @param inputStream InputStream to read from.
   * @return the contents of the InputStream as a String.
   */
  private String inputStreamToString(final InputStream inputStream) throws Exception
  {
    final StringBuilder outputBuilder = new StringBuilder();
      
    try
    {
      String string;
      if (inputStream != null)
      {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, ENCODING));
        while (null != (string = reader.readLine()))
        {
          outputBuilder.append(string).append('\n');
        }
      }
    } 
    catch (Exception ex)
    { throw new Exception("Error reading translation stream : " + ex.toString(), ex);
    }
      
    return outputBuilder.toString();
  }
  
  /** Used to tell Google the origin of the request. */
  protected String m_referrer;
}