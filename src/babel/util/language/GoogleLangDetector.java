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

package babel.util.language;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

import org.json.JSONObject;


/**
 * Uses Google Language API for language detection. Partly based on detection
 * code in google-api-translate-java.
 */
public class GoogleLangDetector implements LangDetector
{
  /** Maximum size of the sting sent to google for identification. */
  protected static final int MAX_TEXT_LENGTH = 1800; // Should be 5000, but longer strings get dropped.
  protected static final String URL = "http://ajax.googleapis.com/ajax/services/language/detect?v=1.0&q=";
  protected static final String ENCODING = "UTF-8";
  protected static byte ESCAPE_CHAR = '%';

  /**
   * @param referrer URL of the the organization originating the request.
   */
  public GoogleLangDetector(String referrer)
  {   
    if (referrer == null || referrer.length() == 0)
    { throw new IllegalArgumentException("Referrer must be supplied.");
    }

    m_referrer = referrer;
  }
  
  /**
   * Detects the language of a supplied string.
   */
  @Override
  public LangDetectionResult detect(final String text) throws Exception
  {
    if (text == null || text.length() == 0)
    { return new LangDetectionResult(null);
    }
        
    JSONObject json = retrieveJSON(new URL(URL + encodeAndTrim(text)));
        
    return new LangDetectionResult(
        Language.fromString(json.getJSONObject("responseData").getString("language")), 
        json.getJSONObject("responseData").getDouble("confidence"),
        json.getJSONObject("responseData").getBoolean("isReliable"));
  }
  
  /**
   * Ancodes and shortens a string taking care not to leave an incomplete unsafe
   * charachter at the end.
   * 
   * @param str original string.
   * @return shortened string.
   */
  protected String encodeAndTrim(String str) throws Exception
  {
    String shortStr = URLEncoder.encode(str, ENCODING);
    
    if (shortStr.length() > MAX_TEXT_LENGTH)
    {
      // Shorten the string
      shortStr = shortStr.substring(0, MAX_TEXT_LENGTH);
      
      // Cut an incomplete unsafe charachter at the end (if any)
      int escIdx = shortStr.lastIndexOf(ESCAPE_CHAR);
      
      if (escIdx > 0 && (MAX_TEXT_LENGTH - escIdx) < 3 )
      { shortStr = shortStr.substring(0, escIdx); 
      }
    }
    
    return shortStr;
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
        final String result = inputStreamToString(uc.getInputStream());
          
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
