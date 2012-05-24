package main.multitask;

import java.io.BufferedReader;
import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.corpora.accessors.CorpusAccessor;
import babel.content.corpora.accessors.LexCorpusAccessor;
import babel.content.eqclasses.EquivalenceClass;
import babel.util.config.Configurator;


public class Learner {
	
	 protected static final Log LOG = LogFactory.getLog(Learner.class);

	
	public void learnStart(int dims, Set<EquivalenceClass> allEqs, ArrayList<Integer> sPositions, int windowSize, double numToks, double learningRate, double initialBias, double wReg, double cReg){

		//dimensionality of C
		m_CDimension=dims;
		Random r = new Random();
		//relative position parameter
		m_W = new HashMap<EquivalenceClass,HashMap<Integer,ArrayList<Double>>> ();
		//word vector representations
		m_C = new HashMap<EquivalenceClass,ArrayList<Double>>();
		//List of s positions (positive and negative)
		m_SPositions = sPositions;
		//should be half the size of sPositions list (e.g. s=[-2,-1,1,2]; m_rightSize=2)
		m_rightSize = windowSize;
		m_leftSize=windowSize;
		//n, same for both updates
		m_LearningRate = learningRate;
		//lambda_w, regularization parameter
		m_regW = wReg;
		//lambda_c, regularization parameter
		m_regC = cReg;
		//number of tokens in training data
		m_numTokens = numToks;
		//initial bias value - what is this???
		//m_InitialBias = initialBias;
		m_InitialBias = 0.0;
		//unsure what this does?
		m_Bias = new HashMap<EquivalenceClass,Double>();
		
		//Initialize W relative position parameters with a number drawn from a Gaussian, divided by 1000
		for (EquivalenceClass eq : allEqs){
			assert m_W.get(eq) == null;
			HashMap<Integer,ArrayList<Double>> newHM = new HashMap<Integer,ArrayList<Double>>();
			m_Bias.put(eq, m_InitialBias);
			for (Integer s : m_SPositions){				
				ArrayList<Double> wvector = new ArrayList<Double>();
				for (int i=0; i<m_CDimension; i++){
					wvector.add(r.nextGaussian()/1000);
				}
				newHM.put(s, wvector);
			}
			m_W.put(eq, newHM);
		}
		
		//Initialize C word vector representation 
		for (EquivalenceClass eq : allEqs){
			assert m_C.get(eq)==null;
			ArrayList<Double> cvector = new ArrayList<Double>();
			for (int i=0; i<m_CDimension; i++){
				cvector.add(r.nextGaussian()/100);
			}
			m_C.put(eq, cvector);
		}
		
	}

	//Update all W_s_x for all x not seen in example
	protected void updateWeightUnseenWords(EquivalenceClass seenWordEQ, int position, EquivalenceClass histWordEq, Double probability){
		Double oldval, cvalforHist, regval, updateval;
		ArrayList<Double> histwordC = m_C.get(histWordEq);
		for (EquivalenceClass eq : m_C.keySet()){
			if (eq!=seenWordEQ){
				HashMap<Integer, ArrayList<Double>> eqsW = m_W.get(eq);
				for (int cdim=0; cdim<m_CDimension; cdim++){
					oldval = eqsW.get(position).get(cdim);
					cvalforHist=histwordC.get(cdim);
					regval=(m_regW*oldval)/m_numTokens;
					updateval = m_LearningRate * ((cvalforHist*(-probability))-regval);
					m_W.get(eq).get(position).set(cdim, (oldval+updateval));
				}
			}
		}
	}
	
	//Update all W_s for all positions and, FOR NOW, only the seen word
	//Update amount: C_word_positionofcontext * [ 1-p(seenWord|history)]
	protected void updateWeightSeenWord(EquivalenceClass seenWordEQ, int position, EquivalenceClass histWordEq, Double probability){
		Double oneMinusProb = 1 - probability;
		ArrayList<Double> histwordC = m_C.get(histWordEq);
		//Iterate through dimensions of C vector of history word
		for (int i=0; i<m_CDimension; i++){
			//Update W_position_seenword
			Double oldval = m_W.get(seenWordEQ).get(position).get(i);
			Double regval = (m_regW*oldval)/m_numTokens;
			Double updateVal = m_LearningRate * ((histwordC.get(i) * oneMinusProb)-regval);
			//System.out.println("oneMinusProb val "+oneMinusProb);
			//System.out.println("histwordC val "+histwordC.get(i));
			//System.out.println("W update val:" +updateVal);
			m_W.get(seenWordEQ).get(position).set(i, oldval + updateVal);
		}
	}

	//Given a seen word, update C value for all others
	protected void updateCUnseenWords(EquivalenceClass seenWordEQ){
		Double myoldval, regval;
		for (EquivalenceClass eq : m_C.keySet()){
			if (eq!=seenWordEQ){
				for (int cdim=0; cdim<m_CDimension; cdim++){
					myoldval=m_C.get(eq).get(cdim);
					regval = m_LearningRate*((m_regC*myoldval)/m_numTokens);
					m_C.get(eq).set(cdim, myoldval-regval);
				}
			
			}
		}
	}
	
	//Update C values for observed x: current plus learning rate * sum over positions (difference w_s - w_expected)
	protected void updateCSeenWord(EquivalenceClass seenWordEQ, HashMap<Integer, EquivalenceClass> pairedHistory, Double normalizer){
		Double mycurval, expectedval, updateval,curcval;
		//Get model expected w
		HashMap<Integer, ArrayList<Double>> expectedW = getExpectedW(pairedHistory, normalizer);
		//Iterate over C positions
		for (int i=0; i<m_CDimension; i++){
			//Calculate update value
			updateval = 0.0;
			curcval = m_C.get(seenWordEQ).get(i);
			//sum over positions, difference between my m_W and expected m_W
			for (Integer sPos : m_SPositions){
				mycurval = m_W.get(seenWordEQ).get(sPos).get(i);
				expectedval = expectedW.get(sPos).get(i);
				//System.out.println("mycurval: "+mycurval);
				//System.out.println("Expected W val: "+expectedval);
				updateval += mycurval - expectedval;
			}
			//subtract reg. term
			Double regval = (m_regC*curcval)/m_numTokens;
			updateval -= regval;
			//Now do the update to C-dimension i
			//System.out.println("C update value: "+updateval);
			m_C.get(seenWordEQ).set(i, curcval+(m_LearningRate*updateval));
		}
	}
	
	//Get model's current expected W for a given history, given denominator
	protected HashMap<Integer,ArrayList<Double>> getExpectedW(HashMap<Integer, EquivalenceClass> pairedHistory, Double normalizer){
		HashMap<Integer,ArrayList<Double>> expectedW = new HashMap<Integer,ArrayList<Double>>();
		//First initialize empty HashMap
		for (Integer s : m_SPositions){
			ArrayList<Double> newlist = new ArrayList<Double>();
			for (int i=0; i<m_CDimension; i++){
				newlist.add(0.0);
			}
			expectedW.put(s, newlist);			
		}
		//Loop over all x' in all eqs:
		for (EquivalenceClass eq : m_C.keySet()){
			//Get probability of that word in the context; returned in non-log space
			Double myprob = getProbWordGivenHistory(pairedHistory, eq, normalizer);
			//for each position in w_eq
			for (Integer s : m_SPositions){
				//For each dimension in w_eq_s
				for (int pos=0; pos<m_CDimension; pos++){
					Double wsx_val = m_W.get(eq).get(s).get(pos);
					Double prevval = expectedW.get(s).get(pos);
					//if (!(prevval<100 || prevval > -100)){
					//	System.out.println(myprob);
					//	System.out.println("prevval "+prevval);
					//	System.out.println("wsx_val "+wsx_val);
					//}
					if (!(Double.isNaN(wsx_val*myprob)) && ((wsx_val*myprob)<Float.POSITIVE_INFINITY) && ((wsx_val*myprob)>Float.NEGATIVE_INFINITY)){
						expectedW.get(s).set(pos, wsx_val*myprob + prevval);
					}
				}
			}
		}
		return expectedW;
	}
	
	//Given pairs of ngram history positions and eq's, return sum of p(x|hist) for all x, to normalize probabilities
	//Return value AS A LOG PROBABILITY (don't exponentiate)
	protected Double getProbDenominator(HashMap<Integer,EquivalenceClass> pairedHistory){
		Double denominator = 0.0;
		//System.out.println(pairedHistory);
		boolean first=true;
		//Loop through all v
		for (EquivalenceClass alleq : m_C.keySet()){
			//System.out.println(alleq.getStem());
			Double sumForWord = m_Bias.get(alleq);
			HashMap<Integer, ArrayList<Double>> wordWbyPos = m_W.get(alleq);
			for (int pos : pairedHistory.keySet()){
				ArrayList<Double> CSigforHist = m_C.get(pairedHistory.get(pos));
				ArrayList<Double> wordPosWeight = wordWbyPos.get(pos);
				assert CSigforHist.size()==wordPosWeight.size();
				Double product = 0.0;
				for (int i = 0; i<m_CDimension; i++){
					Double sumval = CSigforHist.get(i)*wordPosWeight.get(i);
					if (!Double.isNaN(sumval)){
						product += sumval;
					}
				}
				sumForWord += product;
			}
			//Now, instead of summing exponentiated log probs, keep track of sum
			if (first){
				denominator=sumForWord;
				first=false;
			}
			else{
				denominator = addLogs(sumForWord,denominator);
			}
		}
		return denominator;
	}
	
	//If normalizing denominator already calculated, go ahead and return division
	protected Double getNumerator(HashMap<Integer,EquivalenceClass> pairedHistory, EquivalenceClass eqWord, Double normalizer){
		Double numerator = m_Bias.get(eqWord);
		HashMap<Integer, ArrayList<Double>> wordWbyPos = m_W.get(eqWord);
		for (int pos : pairedHistory.keySet()){
			ArrayList<Double> CSigforHist = m_C.get(pairedHistory.get(pos));
			ArrayList<Double> wordPosWeight = wordWbyPos.get(pos);
			assert CSigforHist.size()==wordPosWeight.size();
			Double product = 0.0;
			for (int i = 0; i<CSigforHist.size(); i++){
				Double sumval = CSigforHist.get(i)*wordPosWeight.get(i);
				if (!Double.isNaN(sumval)){
					product += sumval;
				}
			}
			numerator += product;
		}
		return numerator;
	}	
	
	
	//If normalizing denominator already calculated, go ahead and return division
	protected Double getProbWordGivenHistory(HashMap<Integer,EquivalenceClass> pairedHistory, EquivalenceClass eqWord, Double normalizer){
		Double numerator = m_Bias.get(eqWord);
		HashMap<Integer, ArrayList<Double>> wordWbyPos = m_W.get(eqWord);
		for (int pos : pairedHistory.keySet()){
			ArrayList<Double> CSigforHist = m_C.get(pairedHistory.get(pos));
			ArrayList<Double> wordPosWeight = wordWbyPos.get(pos);
			assert CSigforHist.size()==wordPosWeight.size();
			Double product = 0.0;
			for (int i = 0; i<CSigforHist.size(); i++){
				Double sumval = CSigforHist.get(i)*wordPosWeight.get(i);
				if (!Double.isNaN(sumval)){
					product += sumval;
				}
			}
			numerator += product;
		}
		return Math.exp(numerator-normalizer);
	}
	
	//Call normalizer method
	protected Double getProbWordGivenHistory(HashMap<Integer,EquivalenceClass> pairedHistory, EquivalenceClass eqWord){
		Double numerator = m_Bias.get(eqWord);
		HashMap<Integer, ArrayList<Double>> wordWbyPos = m_W.get(eqWord);
		for (int pos : pairedHistory.keySet()){
			ArrayList<Double> CSigforHist = m_C.get(pairedHistory.get(pos));
			ArrayList<Double> wordPosWeight = wordWbyPos.get(pos);
			assert CSigforHist.size()==wordPosWeight.size();
			Double product = 0.0;
			for (int i = 0; i<CSigforHist.size(); i++){
				Double sumval = CSigforHist.get(i)*wordPosWeight.get(i);
				if (!Double.isNaN(sumval)){
					product += sumval;
				}
			}
			numerator += product;
		}
		
		Double denominator = getProbDenominator(pairedHistory);
		return Math.exp(numerator-denominator);

	}
	

	
	//Iterate through dataset, through sentences, through ngrams
	//For each ngram:
	//1. Update weight vector FOR THAT WORD
	public void learnFromData() throws Exception{
	    CorpusAccessor accessor = getAccessor(Configurator.CONFIG.getString("preprocessing.input.Context"), false);    
	    BufferedReader reader = new BufferedReader(accessor.getCorpusReader());
	    String curLine;
	    String[] curSents;
	    String[] curSentTokens;
	    EquivalenceClass foundEq;
	    int min, max, numlines,hundredslines;
	    //Populate hashmap: lookup string, return equivalence class
	    // TODO: Very inefficient - think of something better
	    HashMap<String, EquivalenceClass> eqsMap = new HashMap<String, EquivalenceClass>(m_C.size());
	    for (EquivalenceClass eq : m_C.keySet())
	    {
	      for (String word : eq.getAllWords())
	      { 
	        assert eqsMap.get(word) == null;
	        eqsMap.put(word, eq);
	      }
	    }
	    
	    //keep up with the number of lines and hundreds of lines (for progress printing)
	    numlines=0;
	    hundredslines=0;
	    while ((curLine = reader.readLine()) != null)
	    {
	      numlines+=1;
	      if (numlines==100){
	    	  hundredslines+=1;
	    	  System.out.print(".");
	    	  numlines=0;
  	      }
	      
	      // Split into likely sentences
	      curSents = sentSplit(curLine, accessor.isOneSentencePerLine());
	      //Loop through sentences
	      for (int numSent = 0; numSent < curSents.length; numSent++ )
	      {
		    // Within each sentence, split into words
	        curSentTokens = curSents[numSent].split(WORD_DELIM_REGEX);
	        //Loop through tokens in sentence
	        for (int numToken = 0; numToken < curSentTokens.length; numToken++)
	        {         
	          // Look for the word's equivalence class
	          if (null != (foundEq = eqsMap.get(EquivalenceClass.getWordOfAppropriateForm(curSentTokens[numToken], m_caseSensitive))))               
	          {        
	   
	        	//System.out.println("Updating based on seen word: "+curSentTokens[numToken]);
	        	  
	            // A window around the word (without going into negative indices or longer-than-string indices)
	            min = Math.max(0, numToken - m_leftSize);
	            max = Math.min(numToken + m_rightSize + 1, curSentTokens.length);
	            
	            // Consider all words in the contextual window (except for the word itself). 
	            // Keep track of pairs of 'histories' and positions (lookup position, find equivalence class for corresponding word)
	            HashMap<Integer,EquivalenceClass> pairedHistory = new HashMap<Integer,EquivalenceClass>();
	            for (int contextIdx = min; contextIdx < max; contextIdx++)
	              {	            
	              if (contextIdx != numToken)
	                { 
	            	EquivalenceClass historyword = eqsMap.get(EquivalenceClass.getWordOfAppropriateForm(curSentTokens[contextIdx], m_caseSensitive));
	            	if (historyword!=null){
	            		//s value is seen token's position minus in-window token's position
	            		int sPos=contextIdx-numToken;
	            		pairedHistory.put(sPos, historyword);
	            	}
	                //LOG.info("Adding "+contextword+" as context for: "+curSentTokens[numToken]);
	            	}
	            }
	            Double normalizer = getProbDenominator(pairedHistory);
        		Double numerator = getNumerator(pairedHistory,foundEq,normalizer);
        		Double probWgH = Math.exp(numerator-normalizer);
        		//System.out.println(foundEq.getStem());
        		//System.out.println("Normalizer: "+normalizer);
        		//System.out.println("Numerator: "+numerator);
        		//System.out.println("probWgH: "+probWgH);
        		//Update W_x_s for all positions s, and for now just the observed x
        		for (int contextIdx=min; contextIdx<max; contextIdx++){
	            	if (contextIdx!=numToken){
		            	EquivalenceClass historyword = eqsMap.get(EquivalenceClass.getWordOfAppropriateForm(curSentTokens[contextIdx], m_caseSensitive));
		            	if (historyword!=null){
		            		int sPos=contextIdx-numToken;
		            		updateWeightSeenWord(foundEq,sPos,historyword,probWgH);	            	
		            		updateWeightUnseenWords(foundEq, sPos, historyword, probWgH);
		            	}
	            	}
	            }
        		//Update word feature representation, for now just of observed x
        		updateCSeenWord(foundEq, pairedHistory, normalizer);
        		updateCUnseenWords(foundEq);
	            }
	          }
	        
	        
	      }
	    }
	    
	    //Now find some similar pairs
	    ArrayList<String> examplewords = new ArrayList<String>();
	    examplewords.add("india");
	    examplewords.add("his");
	    examplewords.add("the");
	    examplewords.add("place");
	    examplewords.add("first");
	    EquivalenceClass wordeq;
	    Double cossim;
	    for (String word : examplewords){
	    	HashMap<String,Double> bestsim = new HashMap<String,Double>();
	        ValueComparator bvc =  new ValueComparator(bestsim);
	        TreeMap<String,Double> sorted_map = new TreeMap(bvc);
	    	wordeq = eqsMap.get(EquivalenceClass.getWordOfAppropriateForm(word, m_caseSensitive));
	    	System.out.println("C for word: "+m_C.get(wordeq));
	    	for (EquivalenceClass compareeq : m_C.keySet()){
	    		if (wordeq!=compareeq){
	    			cossim = cosineSimilarity(m_C.get(wordeq),m_C.get(compareeq));
	    			bestsim.put(compareeq.getStem(), cossim);
	    		}
	    	}
	    	sorted_map.putAll(bestsim);
	        System.out.println("Top 10 similar words and similarity scores for "+word+":");
	        int i=0;
	        for (String key : sorted_map.keySet()) {
	        	if (i<10){
	        		System.out.println(key + "\t"+sorted_map.get(key));
	        	}
	        	i+=1;
	        }

	    }
	}
	
	  //Add two log values
	  protected Double addLogs(Double a, Double b){
		  double m = Math.max(a, b);
		  return (Math.log(Math.exp(a-m)+Math.exp(b-m)) + m);
	  }
	
	  protected Double cosineSimilarity(ArrayList<Double> feats1, ArrayList<Double> feats2){
		  Double f1, f2, ss1, ss2, prod;
		  ss1=0.0;
		  ss2=0.0;
		  prod=0.0;
		  for (int cdim=0; cdim<m_CDimension; cdim++){
			  f1=feats1.get(cdim);
			  f2=feats2.get(cdim);
			  ss1+=(f1*f1);
			  ss2+=(f2*f2);
			  prod+=(f1*f2);
		  }
		  return prod/(ss1+ss2);
	  }
	
	  protected CorpusAccessor getAccessor(String kind, boolean src) throws Exception
	  {
	    CorpusAccessor accessor = null;

	    if ("wiki".equals(kind))
	    { accessor = getWikiAccessor(src);
	    }

	    else
	    { LOG.error("Could not find corpus accessor for " + kind);
	    }
	    
	    return accessor;
	  }
	  
	  protected LexCorpusAccessor getWikiAccessor(boolean src)
	  {
	    String path = Configurator.CONFIG.getString("corpora.wiki.Path");
	    boolean oneSentPerLine = Configurator.CONFIG.getBoolean("corpora.wiki.OneSentPerLine");
	    String fileRegExp = src ? Configurator.CONFIG.getString("corpora.wiki.SrcRegExp") : Configurator.CONFIG.getString("corpora.wiki.TrgRegExp");
	  
	    return new LexCorpusAccessor(fileRegExp, appendSep(path), oneSentPerLine);
	  }
	  
	  protected String appendSep(String str)
	  {
	    String ret = (str == null) ? null : str.trim();
	    
	    if (ret != null && ret.length() > 0 && !ret.endsWith(File.separator))
	    { ret += File.separator; 
	    }
	    
	    return ret;
	  }

	  
	  protected String[] sentSplit(String line, boolean oneSentPerLine)
	  {
	    String[] sents;
	    
	    if (oneSentPerLine)
	    { 
	      sents = new String[1];
	      sents[0] = line;
	    }
	    else
	    { 
	      sents = line.split(SENT_DELIM_REGEX);
	    }
	    
	    return sents;
	  }
	
	  class ValueComparator implements Comparator {

		  Map base;
		  public ValueComparator(Map base) {
		      this.base = base;
		  }

		  public int compare(Object a, Object b) {

		    if((Double)base.get(a) < (Double)base.get(b)) {
		      return 1;
		    } else if((Double)base.get(a) == (Double)base.get(b)) {
		      return 0;
		    } else {
		      return -1;
		    }
		  }
		}
	  
	  
	//Dimensionality of C vector for each word. 
	protected int m_CDimension;
	//C parameters: dimensionality |V| * m_CDimension
	protected HashMap<EquivalenceClass,ArrayList<Double>> m_C;
	//W parameters: dimensionality |V| * m_CDimension * |s|
	//Map from EQ to another HashMap, where look up s position and get c feature vector
	protected HashMap<EquivalenceClass,HashMap<Integer,ArrayList<Double>>> m_W;
	//List of s positions
	protected ArrayList<Integer> m_SPositions;
	protected boolean m_caseSensitive = false;
	public static final String SENT_DELIM_REGEX = "[\\|\\.\\?¿!¡]+";    
	public static final String WORD_DELIM_REGEX = "[\\|\\$\\*\\s\"\'\\-\\+=,;:«»{}()<>\\[\\]\\.\\?¿!¡–“”‘’ ]+";
	//Size of window to left and right
	protected int m_leftSize;
	protected int m_rightSize;
	protected Double m_LearningRate;
	protected Double m_regW;
	protected Double m_regC;
	protected double m_numTokens;
	protected double m_InitialBias;
	protected HashMap<EquivalenceClass, Double> m_Bias;
}

