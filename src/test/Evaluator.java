package test;

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
import babel.content.eqclasses.SimpleEquivalenceClass;
import babel.util.config.Configurator;
import babel.util.dict.Dictionary;
import babel.util.dict.ScoredDictionary;
import babel.util.misc.FileList;
import babel.util.misc.PrefixSuffixFileNameFilter;

public class Evaluator
{
  protected static final Log LOG = LogFactory.getLog(Evaluator.class);
  protected static final String PAIRS_FILE_SUFFIX = ".pairs";
  protected static final String EVAL_FILE_SUFFIX = ".eval";
  protected static int[] K = {1, 5, 10, 20, 30, 40, 50, 60, 80, 100};
  
  public static void main(String[] args) throws Exception
  {
    Set<EquivalenceClass> srcEqsToConsider = null;

    LOG.info("\n" + Configurator.getConfigDescriptor());    

    String dictDir = Configurator.CONFIG.getString("resources.dictionaries.Path");
    String testDictFile = Configurator.CONFIG.getString("resources.dictionaries.TestDictionary");
    String outDir = Configurator.CONFIG.getString("output.Path");
    int ridDictNumTrans = Configurator.CONFIG.containsKey("experiments.DictionaryPruneNumTranslations") ? Configurator.CONFIG.getInt("experiments.DictionaryPruneNumTranslations") : -1;
    boolean filterRomanTrg = Configurator.CONFIG.containsKey("experiments.FilterRomanTrg") && Configurator.CONFIG.getBoolean("experiments.FilterRomanTrg");

    String srcWordsFile = outDir + "srcwords.txt";
    
    if (new File(srcWordsFile).canRead())
    {
      srcEqsToConsider = readSrcEqClass(srcWordsFile);
      LOG.info("ONLY looking at " + srcEqsToConsider.size() + " src eqs from " + srcWordsFile);
    }
    
    // Load Test Dictionary
    Dictionary testDict = new Dictionary(dictDir + testDictFile, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, "TestDict", filterRomanTrg);
    testDict.pruneCounts(ridDictNumTrans);
    LOG.info("Using test dictionary: " + testDict.toString());

    // Evaluate all scored pair files
    evalAllPairFiles(srcEqsToConsider, testDict);
  }
  
  protected static void evalAllPairFiles(Set<EquivalenceClass> srcEqsToConsider, Dictionary testDict) throws Exception
  {
    String outDir = Configurator.CONFIG.getString("output.Path");
    FileList files = new FileList(outDir, new PrefixSuffixFileNameFilter(null, PAIRS_FILE_SUFFIX));
    files.gather();
    String outFileName;
    
    for (String fileName : files.getFileNames())
    {
      outFileName = fileName.substring(0, fileName.indexOf(PAIRS_FILE_SUFFIX)).concat(EVAL_FILE_SUFFIX);
      
      LOG.info("Evaluating " + fileName);
      evalPairFile(fileName, outFileName, srcEqsToConsider, testDict);
    }
  }
    
  protected static void evalPairFile(String fileName, String outFileName, Set<EquivalenceClass> srcEqsToConsider, Dictionary testDict) throws Exception
  {
    ScoredDictionary dict = new ScoredDictionary(fileName, SimpleEquivalenceClass.class, SimpleEquivalenceClass.class, false, -1, "", false);
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
  
  protected static Set<EquivalenceClass> readSrcEqClass(String fileName) throws Exception
  {
    HashSet<EquivalenceClass> srcEqs = new HashSet<EquivalenceClass>();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
    String line;
    String[] toks;    
    EquivalenceClass eq;
            
    while (null != (line = reader.readLine()))
    {
      toks = line.split("\\s");
      
      eq = new SimpleEquivalenceClass();
      eq.init(-1, toks[0], false);
      
      srcEqs.add(eq);
    }
    
    reader.close();
    
    return srcEqs;
  }
}
