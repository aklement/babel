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

import babel.util.language.Language;

public class LangDetectionResult
{  
  public LangDetectionResult(final Language language)
  { this(language, null, null);
  }
  
  public LangDetectionResult(final Language language, final Double confidence)
  { this(language, confidence, null);
  }
  
  public LangDetectionResult(final Language language, final Double confidence, final Boolean isReliable)
  {
    m_language = language;
    m_confidence = confidence;
    m_isReliable = isReliable;
  }
  
  public Language language() 
  { return m_language;
  }

  /**
   * @return language id confidence, null if undefined.
   */
  public Double confidence() 
  { return m_confidence;
  }

  /**
   * @return true iff language id is deemed reliable, null if undefined.
   */
  public Boolean isReliable()
  { return m_isReliable;
  }
  
  public String toString()
  {
    StringBuilder strBld = new StringBuilder();
    
    strBld.append(m_language == null ? "undefined" : m_language.toString());
    
    if (m_confidence != null)
    { strBld.append(", confidence = " + m_confidence);
    }
    
    if (m_isReliable != null)
    { strBld.append(", reliable = " + m_isReliable);
    }
    
    return strBld.toString();
  }
  
  /** Detected language. */
  protected Language m_language;
  /** Confidence (between 0.0 and 1.0) in detected language, or null if undefined. */
  protected Double m_confidence;
  /** True iff the language id was deemed reliable, null if undefined. */
  protected Boolean m_isReliable;
}
