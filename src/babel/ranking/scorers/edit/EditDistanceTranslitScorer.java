package babel.ranking.scorers.edit;

import java.util.Set;

import babel.content.eqclasses.EquivalenceClass;
import babel.ranking.scorers.Scorer;
import babel.util.dict.SimpleDictionary;
import babel.util.misc.EditDistance;

/**
 * Computes cosine similarity score between two TimeDistributions.
 */ 
public class EditDistanceTranslitScorer extends Scorer
{
  public EditDistanceTranslitScorer(SimpleDictionary translitDict) {
	  m_translitDict = translitDict;
  }


/**
   * Computes edit distance similarity between stems of two equivalence classes.
   *  
   * @param oneEq first EquivalenceClass
   * @param twoEq second EquivalenceClass
   * 
   * @return similarity score
   */
  public double score(EquivalenceClass oneEq, EquivalenceClass twoEq)
  {

	//Assume don't know which is src and which is trg
	  
	double count = 0;
    double dist = 0;
    
    // We'll just compute the average edit distance across all pairs - expensive!  TODO: a better heuristic?
    for (String oneStr : oneEq.getAllWords())
    {
      for (String twoStr : twoEq.getAllWords())
      {
    	//Get either first string or second string transliterated... if neither in dictionary, just use original. Src should be the first but maybe not
       if (m_translitDict.getAllSrc().contains(oneStr)){
    		//Get all transliterations in the dictionary
    		Set<String> possibleTransliterations = m_translitDict.getTrg(oneStr);
    		//Just use the first transliteration!
    		oneStr = possibleTransliterations.iterator().next();    		
    	}
    	else if (m_translitDict.getAllSrc().contains(twoStr)){
    		//Get all transliterations in the dictionary
    		Set<String> possibleTransliterations = m_translitDict.getTrg(twoStr);
    		//Just use the first transliteration!
    		twoStr = possibleTransliterations.iterator().next();    		    		
    	}
    	dist += EditDistance.distance(oneStr, twoStr);
        count++;
      }
    }
   
    return dist / count;
  }

  
  /**
   * Larger scores mean closer distributions.
   */
  public boolean smallerScoresAreBetter()
  {
    return true;
  }
  
  public void prepare(EquivalenceClass eq) {}

  protected SimpleDictionary m_translitDict;  

}

