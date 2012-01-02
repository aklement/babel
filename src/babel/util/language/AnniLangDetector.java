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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.HashMap;


/**
 * Uses multi-class perceptron weights pre-learned. See Anni's script on COE machines: /home/hltcoe/airvine/langID
 */
public class AnniLangDetector implements LangDetector
{
  /** Maximum size of the sting sent to Bing for identification. */
  protected static final int MAX_TEXT_LENGTH = 3000;
  protected static final String ENCODING = "UTF-8";
  protected static byte ESCAPE_CHAR = '%';
  protected String[] languages = {"af","sq","am","ar","az","eu","bn","bs","bg","my","zh","hr","cs","da","nl","en","et","tl","fi","fr","gl","ka","de","el","he","hi","hu","id","ga","it","ja","kk","ko","ku","ky","la","lv","lt","mk","ms","ml","mt","mn","ne","no","ps","fa","pl","pt","pa","ro","ru","sr","sk","sl","so","es","sw","sv","ta","tt","ti","tl","te","th","bo","tk","tr","uk","ur","uz","ug","vi","cy"};
  
  
  protected HashMap<String, Double> weights = new HashMap<String, Double>();
  protected boolean weightsRead=false;
  //readWeightsFile();

    public void readWeightsFile() throws IOException{	
    	InputStreamReader reader = new InputStreamReader(new FileInputStream("babel/util/language/langidweights"),"UTF-8");
       BufferedReader fin = new BufferedReader(reader);
       String s;
       while ((s=fin.readLine())!=null){
    	   String[] splits=s.split(":::");
    	   if (splits.length==2){
    		   Double weight = Double.parseDouble(splits[1]);
    		   weights.put(splits[0], weight);}
       }
       weightsRead=true;
    }
  
  /**
   * Detects the language of a supplied string.
   */
  @Override
  public LangDetectionResult detect(final String text) throws Exception
  {
	if (weightsRead==false){
		readWeightsFile();
	}

	if (text == null || text.length() == 0)
    { return new LangDetectionResult(null);
    }
         
    return new LangDetectionResult(Language.fromString(getLanguage(encodeAndTrim(text))));
  }
  
  /**
   * Encodes and shortens a string taking care not to leave an incomplete unsafe
   * character at the end.
   * 
   * @param str original string.
   * @return shortened string.
   */
  protected String encodeAndTrim(String str) throws Exception
  {
    String shortStr = str;
    
    if (shortStr.length() > MAX_TEXT_LENGTH)
    {
     // Pick a shorter sub-string from the text block
     int startIdx = shortStr.length() - MAX_TEXT_LENGTH;
     
     if (startIdx > shortStr.length() / 2) {
    	 startIdx = shortStr.length() / 2;
     }
   
     //if (startIdx != 0) {
     //	 startIdx = shortStr.indexOf(" ", startIdx); 
     //}

      // Shorten the string
      shortStr = shortStr.substring(startIdx, startIdx + MAX_TEXT_LENGTH);
      
      // Cut an incomplete unsafe character at the end (if any)
      int escIdx = shortStr.lastIndexOf(ESCAPE_CHAR);
      
      if (escIdx > 0 && (MAX_TEXT_LENGTH - escIdx) < 3 )
      { shortStr = shortStr.substring(0, escIdx); 
      }
    }
    
    return URLEncoder.encode(shortStr, ENCODING);
  }

  protected String getLanguage(String text) throws Exception
  { 	  

	  Double bestscore = -1000000.0;
	  String bestlabel = "unk";
	  for (String lang : languages){  
		Double myscore = 0.0;
		text=text.replace("+", " ");
		String[] words = text.split(" ");
		for (String w : words){
			if (weights.containsKey(w+"_"+lang)){
				myscore+=weights.get(w+"_"+lang);
			}
		}
		if (text.length()>2){
			int index=3;
			String trigram=text.substring(index-3, index);
			if (weights.containsKey(trigram+"_"+lang)){
				myscore+=weights.get(trigram+"_"+lang);
			}
		}
		if (myscore>bestscore){
			bestlabel=lang;
			bestscore=myscore;
		}
	  }
	  return bestlabel;
  }
    
  public static void main(String[] args) throws Exception {
	  
	  AnniLangDetector detector = new AnniLangDetector();
	  
	  System.out.println(detector.languages.length+"-way language classification");
	  System.out.println("Language = " + detector.detect("Für Bundeskanzlerin Angela Merkel scheint die Sache klar zu sein. Der mögliche Verkauf der Boote hat ihrer Meinung nach nichts mit einem anrüchigen Waffengeschäft zu tun, sondern mit dem legitimen Interesse eines Landes, seine maritimen Grenzen und seine Küsten zu schützen. Deshalb glaube ich nicht, dass wir hier im umfassenden Sinne Aufrüstung betreiben, betonte Merkel während ihres Kurzbesuchs in der angolanischen Hauptstadt Luanda.").toString());
	  System.out.println("Language = " + detector.detect("S&P, que colocó la máxima nota estadounidense en revisión con implicancias negativas, advirtió que podría rebajar la calificación este mes si las conversaciones entre la Casa Blanca y los republicanos siguen estancadas. Cualquier recorte podría ser en uno o más escalones, agregó la firma.").toString());	  
	  System.out.println("Language = " + detector.detect("i like to classify").toString());
	  System.out.println("Language = " + detector.detect("me gusta mexico te amo me vivo en baltimore").toString());
	  System.out.println("Language = " + detector.detect("oninkrijk. Het kanaal begrenst het sch").toString());
  }
}
