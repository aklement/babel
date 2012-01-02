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

/**
 * Modified from the original in google-api-translate-java distributed under
 * GNU General Public License.
 */

/**
 * Language information.
 */
public enum Language
{
	AUTO_DETECT(""),
  AFRIKAANS("af"), //Wikipedia - a01
  ALBANIAN("sq"), //Have from previous crawls
  AMHARIC("am"), //Have from previous crawls
  ARABIC("ar"), //Have from previous crawls
  ARMENIAN("hy"),
  AZERBAIJANI("az"), //Have from previous crawls
  BASQUE("eu"), //Wikipedia - a01
  BELARUSIAN("be"),
  BENGALI("bn"), //Have from previous crawls
  BOSNIAN("bs"), //Have from previous crawls
  BIHARI("bh"),
  BULGARIAN("bg"), //Have from previous crawls
  BURMESE("my"), //Wikipedia - a05
  CATALAN("ca"),
  CHEROKEE("chr"),
  CHINESE("zh"), //Have from previous crawls
  CHINESE_SIMPLIFIED("zh-CN"),
  CHINESE_TRADITIONAL("zh-TW"),
  CROATIAN("hr"), //Have from previous crawls
  CZECH("cs"), //Wikipydia extracted
  DANISH("da"), //Wikipydia extracted
  DHIVEHI("dv"),
  DUTCH("nl"), //Wikipydia extracted
  ENGLISH("en"), //Have from previous crawls
  ESPERANTO("eo"),
  ESTONIAN("et"), //Wikipydia extracted
  FILIPINO("tl"), //Wikipedia - a03
  FINNISH("fi"), //Wikipydia extracted
  FRENCH("fr"), //Have from previous crawls
  GALICIAN("gl"), //Have from previous crawls
  GEORGIAN("ka"), //Wikipydia extracted
  GERMAN("de"), //Have from previous crawls
  GREEK("el"), //Have from previous crawls
  GUARANI("gn"),
  GUJARATI("gu"),
  HEBREW("he"), //Wikipydia extracted and LANG CODE CHANGED
  //HEBREW("iw"),
  HINDI("hi"), //Have from previous crawls
  HUNGARIAN("hu"), //Wikipydia extracted
  ICELANDIC("is"),
  INDONESIAN("id"), //Have from previous crawls
  INUKTITUT("iu"),
  IRISH("ga"), // Wikipedia - a03	
  ITALIAN("it"), //Have from previous crawls
  JAPANESE("ja"), //Wikipydia extracted
  //Have Kapampangan Wikipedia - a03
  KANNADA("kn"),
  KAZAKH("kk"), //Have from previous crawls
  KHMER("km"),
  KOREAN("ko"), //Wikipedia - a02
  KURDISH("ku"), //Wikipedia - a02
  KYRGYZ("ky"), //Have from previous crawls
  LATIN("la"), //Wikipedia - a02 - wasn't listed before
  LAOTHIAN("lo"),
  LATVIAN("lv"), //Have from previous crawls
  LITHUANIAN("lt"), //Wikipydia extracted
  MACEDONIAN("mk"), //Have from previous crawls
  MALAY("ms"), //Have from previous crawls
  MALAYALAM("ml"), //Wikipydia extracted
  MALTESE("mt"),  //Wikipedia - a05
  MARATHI("mr"),
  MONGOLIAN("mn"), //Wikipedia - a05
  NEPALI("ne"), //Have from previous crawls
  NORWEGIAN("no"), //Wikipydia extracted
  ORIYA("or"),
  PASHTO("ps"), //Have from previous crawls
  PERSIAN("fa"), //Have from previous crawls
  POLISH("pl"), //Have from previous crawls
  PORTUGUESE("pt"), //Have from previous crawls
  PUNJABI("pa"), //Wikipedia - a05
  ROMANIAN("ro"), //Have from previous crawls
  RUSSIAN("ru"), //Have from previous crawls
  SANSKRIT("sa"),
  SERBIAN("sr"), //Have from previous crawls
  SINDHI("sd"),
  SINHALESE("si"),
  SLOVAK("sk"), //Have from previous crawls
  SLOVENIAN("sl"), //Wikipydia extracted
  SOMALI("so"), //Have from previous crawls
  SPANISH("es"), //Have from previous crawls
  SWAHILI("sw"), //Have from previous crawls
  SWEDISH("sv"), //Wikipydia extracted
  TAJIK("tg"),
  TAMIL("ta"), //Have from previous crawls
  TATAR("tt"), //Wikipedia - a03 - wasn't listed before
  TIGRINYA("ti"), //Wikipedia - a05 - wasn't listed before
  TAGALOG("tl"), //Wikipedia - a03
  TELUGU("te"), //Wikipydia extracted
  THAI("th"), //Wikipedia - a05
  TIBETAN("bo"), //Wikipedia - a05
  TURKMEN("tk"), //Wikipedia - a02 - wasn't listed before
  TURKISH("tr"), //Have from previous crawls
  UKRANIAN("uk"), //Have from previous crawls
  URDU("ur"), //Have from previous crawls
  UZBEK("uz"), //Havefrom previous crawls
  UIGHUR("ug"), //Wikipedia - a02
  VIETNAMESE("vi"), //Wikipydia extracted
  WELSH("cy"), //Have from previous crawls
  YIDDISH("yi");
	  
	
	
  /**
   * Enum constructor.
   * @param pLanguage The language identifier.
   */
  private Language(final String language)
  {
    m_language = language;
  }
  
  public static Language fromString(final String language)
  {
    if (language != null)
    {
      for (Language l : values())
      {
        if (language.equals(l.toString()))
        { return l;
        }
      }
    }
    
    return null;
  }
  
  /**
   * Returns the String representation of this language.
   * @return The String representation of this language.
   */
  @Override
  public String toString()
  {
    return m_language;
  }
  
  /** String representation of this language. */
  private final String m_language;
}