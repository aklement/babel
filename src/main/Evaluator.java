package main;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import babel.content.eqclasses.EquivalenceClass;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.ScoredDictionary;

public class Evaluator
{
  protected static final Log LOG = LogFactory.getLog(Evaluator.class);
  protected static final String PAIRS_FILE_SUFFIX = ".pairs";
  protected static final String EVAL_FILE_SUFFIX = ".eval";
  protected static int[] K = {1, 5, 10, 20, 30, 40, 50, 60, 80, 100};
  
  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception
  {
    Set<EquivalenceClass> srcEqsToConsider = null;

    LOG.info("\n" + Configurator.getConfigDescriptor());    

    String dictDir = Configurator.CONFIG.getString("resources.dictionaries.Path");
    String testDictFile = Configurator.CONFIG.getString("resources.dictionaries.TestDictionary");
    String outDir = Configurator.CONFIG.getString("output.Path");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
    String srcEqClasses = Configurator.CONFIG.getString("preprocessing.SrcEqClass");
    String trgEqClasses = Configurator.CONFIG.getString("preprocessing.TrgEqClass");
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("preprocessing.FilterRomanTrg") && Configurator.CONFIG.getBoolean("preprocessing.FilterRomanTrg");
    Class<? extends EquivalenceClass> srcEqClassClass = (Class<? extends EquivalenceClass>)Class.forName(srcEqClasses);
    Class<? extends EquivalenceClass> trgEqClassClass = (Class<? extends EquivalenceClass>)Class.forName(trgEqClasses);
    
    String srcWordsFile = outDir + "srcwords.txt";
    
    if (!new File(srcWordsFile).canRead())
    { throw new IllegalArgumentException("Cannot read " + srcWordsFile);
    }
    
    srcEqsToConsider = readSrcEqClass(srcEqClassClass, srcWordsFile);
    LOG.info("ONLY looking at " + srcEqsToConsider.size() + " src eqs from " + srcWordsFile);
    
    Dictionary testDict = new Dictionary(dictDir + testDictFile, srcEqClassClass, trgEqClassClass, "TestDict", filterRomanTrg);
    testDict.pruneCounts(ridDictNumTrans);
    LOG.info("Using test dictionary: " + testDict.toString());

    evalAllPairFiles(outDir + "time" + PAIRS_FILE_SUFFIX, false, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
    evalAllPairFiles(outDir + "context" + PAIRS_FILE_SUFFIX, false, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
    evalAllPairFiles(outDir + "time-context" + PAIRS_FILE_SUFFIX, false, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
    evalAllPairFiles(outDir + "context-time" + PAIRS_FILE_SUFFIX, false, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
    evalAllPairFiles(outDir + "edit" + PAIRS_FILE_SUFFIX, true, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
    evalAllPairFiles(outDir + "aggmrr" + PAIRS_FILE_SUFFIX, false, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
  }
  
  protected static void evalAllPairFiles(String fileName, boolean smallerScoresAreBetter, Class<? extends EquivalenceClass> srcEqClassClass, Class<? extends EquivalenceClass> trgEqClassClass, Set<EquivalenceClass> srcEqsToConsider, Dictionary testDict) throws Exception
  {
    LOG.info("Evaluating " + fileName);
    String outFileName = fileName.substring(0, fileName.indexOf(PAIRS_FILE_SUFFIX)).concat(EVAL_FILE_SUFFIX);
    evalPairFile(fileName, smallerScoresAreBetter, outFileName, srcEqClassClass, trgEqClassClass, srcEqsToConsider, testDict);
  }
    
  protected static void evalPairFile(String fileName, boolean smallerScoresAreBetter, String outFileName, Class<? extends EquivalenceClass> srcEqClassClass, Class<? extends EquivalenceClass> trgEqClassClass, Set<EquivalenceClass> srcEqsToConsider, Dictionary testDict) throws Exception
  {
    ScoredDictionary dict = new ScoredDictionary(fileName, srcEqClassClass, trgEqClassClass, smallerScoresAreBetter, -1, "", false);
    Set<EquivalenceClass> srcEqs = dict.getAllKeys();
    Collection<EquivalenceClass> goldTrans;  
    double numInTopK, oneInTopK;
    double curCount, total;
    double accInTopK, precInTopK;
    
    if (srcEqsToConsider != null)
    { srcEqs.retainAll(srcEqsToConsider);
    }
    
    // Evaluate
    BufferedWriter writer = new BufferedWriter(new FileWriter(outFileName));
    DecimalFormat df = new DecimalFormat("0.00");

    writer.write("K\tAccuracy@TopK\tPrec@TopK\tNumInDict");
    writer.newLine();
    
    for (int i = 0; i < K.length; i++)
    {
      numInTopK = 0;
      oneInTopK = 0;
      total = 0;
      
      for (EquivalenceClass srcEq : srcEqs)
      {
        goldTrans = testDict.getTranslations(srcEq);
        
        if (goldTrans != null)
        {
          curCount = dict.numInTopK(srcEq, goldTrans, K[i]);
          numInTopK += curCount;
          oneInTopK += (curCount > 0) ? 1 : 0;
          total++;
        }
      }
      
      accInTopK = 100.0 * oneInTopK / total;
      precInTopK = 100.0 * numInTopK / (total * K[i]);
    
      writer.write(K[i] + "\t" + df.format(accInTopK) + "\t" + df.format(precInTopK) + "\t" + total);
      writer.newLine();
    }
    
    writer.close();
  }
  
  protected static Set<EquivalenceClass> readSrcEqClass(Class<? extends EquivalenceClass> srcEqClassClass, String fileName) throws Exception
  {
    HashSet<EquivalenceClass> srcEqs = new HashSet<EquivalenceClass>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
    String line;
    String[] toks;    
    EquivalenceClass eq;
            
    while (null != (line = reader.readLine()))
    {
      toks = line.split("\\s");
      
      eq = srcEqClassClass.newInstance();
      eq.init(-1, toks[0], false);
      
      srcEqs.add(eq);
    }
    
    reader.close();
    
    return srcEqs;
  }
}
