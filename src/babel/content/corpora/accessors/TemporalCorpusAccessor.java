package babel.content.corpora.accessors;

import java.io.InputStreamReader;
import java.util.Date;

/**
 * Adds a "per day" way of accessing the corpus.
 */
public abstract class TemporalCorpusAccessor extends CorpusAccessor
{
  protected TemporalCorpusAccessor(boolean oneSentPerLine)
  { super(oneSentPerLine);
  }

  /**
   * Resets the per day view. Note that nextDay() must be called before getting
   * the first per-day stream. 
   * @return true iff reset succeeded.
   */
  public abstract boolean resetDays();
  
  /**
   * @return true iff corpus contains data for the next day.
   */
  public abstract boolean nextDay();

	/**
	 * @return an InputStream reader for a current day view of the corpus.
	 */
  public abstract InputStreamReader getCurDayReader();
  
  public abstract Date getCurDay();
}