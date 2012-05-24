package main.multitask;


import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.util.config.Configurator;

public class monoLearn {
	  public static final Log LOG = LogFactory.getLog(monoLearn.class);

	  public static void main(String[] args) throws Exception
	  {
		  LOG.info("\n" + Configurator.getConfigDescriptor());
		  monoLearn monolearner = new monoLearn();
		  boolean learnTrgOnly = true;
		  monolearner.learn(learnTrgOnly);  
	  }

	  protected void learn(boolean learnTrgOnly) throws Exception{
		  //Collect context properties: position sensitive and target language only
		  MTDataPreparer preparer = new MTDataPreparer();
		  preparer.prepare(learnTrgOnly);
		  double numTrgTokens = preparer.getNumTrgTokens();

		  double initialBias = Configurator.CONFIG.getDouble("preprocessing.learnParameters.initialBias");
		  double learningRate = Configurator.CONFIG.getDouble("preprocessing.learnParameters.learningRate");
		  double wreg = Configurator.CONFIG.getDouble("preprocessing.learnParameters.wReg");
		  double creg = Configurator.CONFIG.getDouble("preprocessing.learnParameters.cReg");
		  int windowsize = Configurator.CONFIG.getInt("preprocessing.learnParameters.windowSize");
		  int cDimension = Configurator.CONFIG.getInt("preprocessing.learnParameters.cDimension");

		  Learner learner = new Learner();
		  //Dimensionality of C, set of equivalence classes, set of s positions, window size to left and right
		  ArrayList<Integer> positions = new ArrayList<Integer>();
		  for (int i=1; i<=windowsize; i++){
			  positions.add(-1*i);
			  positions.add(1*i);
		  }
		  learner.learnStart(cDimension, preparer.m_trgEqs, positions,windowsize,numTrgTokens, learningRate, initialBias, wreg, creg);
		  learner.learnFromData();
		  
	  }
	  
	
}
