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

import org.apache.hadoop.mapred.JobConf;
import org.apache.nutch.analysis.lang.LanguageIdentifier;

/**
 * Wraps Nutch language identifier plugin.
 */
public class NutchLangDetector implements LangDetector
{
  public NutchLangDetector(JobConf job)
  {
    m_langID = new LanguageIdentifier(job);
  }
  
  public LangDetectionResult detect(final String text) throws Exception
  {
    if (text == null || text.length() == 0)
    { return new LangDetectionResult(null);
    }
    
    return new LangDetectionResult(Language.fromString(m_langID.identify(text)));
  }

  protected LanguageIdentifier m_langID;
}
