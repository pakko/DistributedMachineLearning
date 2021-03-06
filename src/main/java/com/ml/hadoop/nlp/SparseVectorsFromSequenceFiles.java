/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
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

package com.ml.hadoop.nlp;

import org.apache.commons.cli2.CommandLine;
import org.apache.commons.cli2.Group;
import org.apache.commons.cli2.Option;
import org.apache.commons.cli2.OptionException;
import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.cli2.builder.GroupBuilder;
import org.apache.commons.cli2.commandline.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.CommandLineUtil;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.Pair;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.common.iterator.sequencefile.SequenceFileIterable;
import org.apache.mahout.common.lucene.AnalyzerUtils;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.hadoop.stats.BasicStats;
import org.apache.mahout.vectorizer.HighDFWordsPruner;
import org.apache.mahout.vectorizer.collocations.llr.LLRReducer;
import org.apache.mahout.vectorizer.common.PartialVectorMerger;
import org.apache.mahout.vectorizer.tfidf.TFIDFConverter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * Converts a given set of sequence files into SparseVectors
 */
public final class SparseVectorsFromSequenceFiles extends AbstractJob {

  private static final Logger log = LoggerFactory.getLogger(SparseVectorsFromSequenceFiles.class);

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new SparseVectorsFromSequenceFiles(), args);
  }

  @Override
  public int run(String[] args) throws Exception {
    DefaultOptionBuilder obuilder = new DefaultOptionBuilder();
    ArgumentBuilder abuilder = new ArgumentBuilder();
    GroupBuilder gbuilder = new GroupBuilder();

    Option inputDirOpt = DefaultOptionCreator.inputOption().create();

    Option outputDirOpt = DefaultOptionCreator.outputOption().create();

    Option minSupportOpt = obuilder.withLongName("minSupport").withArgument(
            abuilder.withName("minSupport").withMinimum(1).withMaximum(1).create()).withDescription(
            "(Optional) Minimum Support. Default Value: 2").withShortName("s").create();

    Option analyzerNameOpt = obuilder.withLongName("analyzerName").withArgument(
            abuilder.withName("analyzerName").withMinimum(1).withMaximum(1).create()).withDescription(
            "The class name of the analyzer").withShortName("a").create();
    
    Option dictionaryPathOpt = obuilder.withLongName("dictionaryPath").withArgument(
            abuilder.withName("dictionaryPath").withMinimum(1).withMaximum(1).create()).withDescription(
            "Dictionary path for update TFIDF").withShortName("dp").create();
    
    Option docFrequencyPathOpt = obuilder.withLongName("docFrequencyPath").withArgument(
            abuilder.withName("docFrequencyPath").withMinimum(1).withMaximum(1).create()).withDescription(
            "Doc frequency path for update TFIDF").withShortName("dfp").create();

    Option tfVectorsPathOpt = obuilder.withLongName("tfVectorsPath").withArgument(
            abuilder.withName("tfVectorsPath").withMinimum(1).withMaximum(1).create()).withDescription(
            "TF Vectors path").withShortName("tfvp").create();
    
    Option chunkSizeOpt = obuilder.withLongName("chunkSize").withArgument(
            abuilder.withName("chunkSize").withMinimum(1).withMaximum(1).create()).withDescription(
            "The chunkSize in MegaBytes. 100-10000 MB").withShortName("chunk").create();

    Option weightOpt = obuilder.withLongName("weight").withRequired(false).withArgument(
            abuilder.withName("weight").withMinimum(1).withMaximum(1).create()).withDescription(
            "The kind of weight to use. Currently TF , TFIDF or TFIDF_UPDATE").withShortName("wt").create();

    Option minDFOpt = obuilder.withLongName("minDF").withRequired(false).withArgument(
            abuilder.withName("minDF").withMinimum(1).withMaximum(1).create()).withDescription(
            "The minimum document frequency.  Default is 1").withShortName("md").create();

    Option maxDFPercentOpt = obuilder.withLongName("maxDFPercent").withRequired(false).withArgument(
            abuilder.withName("maxDFPercent").withMinimum(1).withMaximum(1).create()).withDescription(
            "The max percentage of docs for the DF.  Can be used to remove really high frequency terms."
                    + " Expressed as an integer between 0 and 100. Default is 99.  If maxDFSigma is also set, "
                    + "it will override this value.").withShortName("x").create();

    Option maxDFSigmaOpt = obuilder.withLongName("maxDFSigma").withRequired(false).withArgument(
            abuilder.withName("maxDFSigma").withMinimum(1).withMaximum(1).create()).withDescription(
            "What portion of the tf (tf-idf) vectors to be used, expressed in times the standard deviation (sigma) "
                    + "of the document frequencies of these vectors. Can be used to remove really high frequency terms."
                    + " Expressed as a double value. Good value to be specified is 3.0. In case the value is less "
                    + "than 0 no vectors will be filtered out. Default is -1.0.  Overrides maxDFPercent")
            .withShortName("xs").create();

    Option minLLROpt = obuilder.withLongName("minLLR").withRequired(false).withArgument(
            abuilder.withName("minLLR").withMinimum(1).withMaximum(1).create()).withDescription(
            "(Optional)The minimum Log Likelihood Ratio(Float)  Default is " + LLRReducer.DEFAULT_MIN_LLR)
            .withShortName("ml").create();

    Option numReduceTasksOpt = obuilder.withLongName("numReducers").withArgument(
            abuilder.withName("numReducers").withMinimum(1).withMaximum(1).create()).withDescription(
            "(Optional) Number of reduce tasks. Default Value: 1").withShortName("nr").create();

    Option powerOpt = obuilder.withLongName("norm").withRequired(false).withArgument(
            abuilder.withName("norm").withMinimum(1).withMaximum(1).create()).withDescription(
            "The norm to use, expressed as either a float or \"INF\" if you want to use the Infinite norm.  "
                    + "Must be greater or equal to 0.  The default is not to normalize").withShortName("n").create();

    Option logNormalizeOpt = obuilder.withLongName("logNormalize").withRequired(false)
            .withDescription(
                    "(Optional) Whether output vectors should be logNormalize. If set true else false")
            .withShortName("lnorm").create();

    Option maxNGramSizeOpt = obuilder.withLongName("maxNGramSize").withRequired(false).withArgument(
            abuilder.withName("ngramSize").withMinimum(1).withMaximum(1).create())
            .withDescription(
                    "(Optional) The maximum size of ngrams to create"
                            + " (2 = bigrams, 3 = trigrams, etc) Default Value:1").withShortName("ng").create();

    Option sequentialAccessVectorOpt = obuilder.withLongName("sequentialAccessVector").withRequired(false)
            .withDescription(
                    "(Optional) Whether output vectors should be SequentialAccessVectors. If set true else false")
            .withShortName("seq").create();

    Option namedVectorOpt = obuilder.withLongName("namedVector").withRequired(false)
            .withDescription(
                    "(Optional) Whether output vectors should be NamedVectors. If set true else false")
            .withShortName("nv").create();

    Option overwriteOutput = obuilder.withLongName("overwrite").withRequired(false).withDescription(
            "If set, overwrite the output directory").withShortName("ow").create();
    Option helpOpt = obuilder.withLongName("help").withDescription("Print out help").withShortName("h")
            .create();

    Group group = gbuilder.withName("Options").withOption(minSupportOpt).withOption(analyzerNameOpt)
    		.withOption(dictionaryPathOpt).withOption(docFrequencyPathOpt).withOption(tfVectorsPathOpt)
            .withOption(chunkSizeOpt).withOption(outputDirOpt).withOption(inputDirOpt).withOption(minDFOpt)
            .withOption(maxDFSigmaOpt).withOption(maxDFPercentOpt).withOption(weightOpt).withOption(powerOpt)
            .withOption(minLLROpt).withOption(numReduceTasksOpt).withOption(maxNGramSizeOpt).withOption(overwriteOutput)
            .withOption(helpOpt).withOption(sequentialAccessVectorOpt).withOption(namedVectorOpt)
            .withOption(logNormalizeOpt)
            .create();
    try {
      Parser parser = new Parser();
      parser.setGroup(group);
      parser.setHelpOption(helpOpt);
      CommandLine cmdLine = parser.parse(args);

      if (cmdLine.hasOption(helpOpt)) {
        CommandLineUtil.printHelp(group);
        return -1;
      }

      Path inputDir = new Path((String) cmdLine.getValue(inputDirOpt));
      Path outputDir = new Path((String) cmdLine.getValue(outputDirOpt));

      int chunkSize = 100;
      if (cmdLine.hasOption(chunkSizeOpt)) {
        chunkSize = Integer.parseInt((String) cmdLine.getValue(chunkSizeOpt));
      }
      int minSupport = 2;
      if (cmdLine.hasOption(minSupportOpt)) {
        String minSupportString = (String) cmdLine.getValue(minSupportOpt);
        minSupport = Integer.parseInt(minSupportString);
      }

      int maxNGramSize = 1;

      if (cmdLine.hasOption(maxNGramSizeOpt)) {
        try {
          maxNGramSize = Integer.parseInt(cmdLine.getValue(maxNGramSizeOpt).toString());
        } catch (NumberFormatException ex) {
          log.warn("Could not parse ngram size option");
        }
      }
      log.info("Maximum n-gram size is: {}", maxNGramSize);

      if (cmdLine.hasOption(overwriteOutput)) {
        HadoopUtil.delete(getConf(), outputDir);
      }

      float minLLRValue = LLRReducer.DEFAULT_MIN_LLR;
      if (cmdLine.hasOption(minLLROpt)) {
        minLLRValue = Float.parseFloat(cmdLine.getValue(minLLROpt).toString());
      }
      log.info("Minimum LLR value: {}", minLLRValue);

      int reduceTasks = 1;
      if (cmdLine.hasOption(numReduceTasksOpt)) {
        reduceTasks = Integer.parseInt(cmdLine.getValue(numReduceTasksOpt).toString());
      }
      log.info("Changed... Number of reduce tasks: {}", reduceTasks);

      Class<? extends Analyzer> analyzerClass = StandardAnalyzer.class;
      if (cmdLine.hasOption(analyzerNameOpt)) {
        String className = cmdLine.getValue(analyzerNameOpt).toString();
        analyzerClass = Class.forName(className).asSubclass(Analyzer.class);
        // try instantiating it, b/c there isn't any point in setting it if
        // you can't instantiate it
        AnalyzerUtils.createAnalyzer(analyzerClass);
      }

      //default process tfidf:1, tf:2, update tfidf:3
      int processIdf;

      if (cmdLine.hasOption(weightOpt)) {
        String wString = cmdLine.getValue(weightOpt).toString();
        if ("tf".equalsIgnoreCase(wString)) {
          processIdf = 2;
        } else if ("tfidf".equalsIgnoreCase(wString)) {
          processIdf = 1;
        } else if ("tfidf_update".equalsIgnoreCase(wString)) {
          processIdf = 3;
        } else { 
          throw new OptionException(weightOpt);
        }
      } else {
        processIdf = 1;
      }

      int minDf = 1;
      if (cmdLine.hasOption(minDFOpt)) {
        minDf = Integer.parseInt(cmdLine.getValue(minDFOpt).toString());
      }
      int maxDFPercent = 99;
      if (cmdLine.hasOption(maxDFPercentOpt)) {
        maxDFPercent = Integer.parseInt(cmdLine.getValue(maxDFPercentOpt).toString());
      }
      double maxDFSigma = -1.0;
      if (cmdLine.hasOption(maxDFSigmaOpt)) {
        maxDFSigma = Double.parseDouble(cmdLine.getValue(maxDFSigmaOpt).toString());
      }

      float norm = PartialVectorMerger.NO_NORMALIZING;
      if (cmdLine.hasOption(powerOpt)) {
        String power = cmdLine.getValue(powerOpt).toString();
        if ("INF".equals(power)) {
          norm = Float.POSITIVE_INFINITY;
        } else {
          norm = Float.parseFloat(power);
        }
      }

      boolean logNormalize = false;
      if (cmdLine.hasOption(logNormalizeOpt)) {
        logNormalize = true;
      }
      log.info("Tokenizing documents in {}", inputDir);
      Configuration conf = getConf();
      Path tokenizedPath = new Path(outputDir, DocumentProcessor.TOKENIZED_DOCUMENT_OUTPUT_FOLDER);
      DocumentProcessor.tokenizeDocuments(inputDir, analyzerClass, tokenizedPath, conf);

      boolean sequentialAccessOutput = false;
      if (cmdLine.hasOption(sequentialAccessVectorOpt)) {
        sequentialAccessOutput = true;
      }

      boolean namedVectors = false;
      if (cmdLine.hasOption(namedVectorOpt)) {
        namedVectors = true;
      }
      boolean shouldPrune = maxDFSigma >= 0.0 || maxDFPercent > 0.00;
      String tfDirName = shouldPrune
              ? DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-toprune"
              : DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER;
      log.info("Creating Term Frequency Vectors, prune {}", shouldPrune);
      
      String dictionaryPath = null;
      if (cmdLine.hasOption(dictionaryPathOpt)) {
          dictionaryPath = (String) cmdLine.getValue(dictionaryPathOpt);
          log.info("begin dic path {}", dictionaryPath);
      }
      
      if (processIdf == 1) {
        DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
                outputDir,
                tfDirName,
                conf,
                minSupport,
                maxNGramSize,
                minLLRValue,
                -1.0f,
                false,
                reduceTasks,
                chunkSize,
                sequentialAccessOutput,
                namedVectors);
      } else if(processIdf == 3) {
    	  log.info("begin update term----------------");
    	  DictionaryVectorizer.createUpdateTermFrequencyVectors(tokenizedPath,
                  outputDir,
                  tfDirName,
                  conf,
                  maxNGramSize,
                  dictionaryPath,
                  norm,
                  logNormalize,
                  reduceTasks,
                  sequentialAccessOutput,
                  namedVectors);
      } else {
        DictionaryVectorizer.createTermFrequencyVectors(tokenizedPath,
                outputDir,
                tfDirName,
                conf,
                minSupport,
                maxNGramSize,
                minLLRValue,
                norm,
                logNormalize,
                reduceTasks,
                chunkSize,
                sequentialAccessOutput,
                namedVectors);
      }

      
      String docFrequencyPaths = null;
	  if (cmdLine.hasOption(dictionaryPathOpt)) {
		  docFrequencyPaths = (String) cmdLine.getValue(docFrequencyPathOpt);
		  log.info("doc frequency path {}", docFrequencyPaths);
	  }
	  String tfVectorsPaths = null;
	  if (cmdLine.hasOption(tfVectorsPathOpt)) {
		  tfVectorsPaths = (String) cmdLine.getValue(tfVectorsPathOpt);
		  log.info("tf vectors path {}", tfVectorsPaths);
	  }
		
      Pair<Long[], List<Path>> docFrequenciesFeatures = null;
      // Should document frequency features be processed
		if (processIdf == 1) {
			log.info("Calculating IDF");
			docFrequenciesFeatures = TFIDFConverter.calculateDF(new Path(
					outputDir, tfDirName), outputDir, conf, chunkSize);
			log.info("...docFrequencyPathBase {}, docFrequencyFile {}", docFrequenciesFeatures.getFirst()[0], docFrequenciesFeatures.getFirst()[1]);
		} else if (processIdf == 3) {
			// load docFrequency path
			List<Path> docFrequencyChunks = Lists.newArrayList();
			String[] paths = docFrequencyPaths.split(",");
			
			long featureCount = 0;
			for (String path : paths) {
				int splitPos = path.lastIndexOf("/");
				String docFrequencyPathBase = path.substring(0, splitPos);
				String docFrequencyFile = path.substring(splitPos + 1,
						path.length());
				log.info("docFrequencyPathBase {}, docFrequencyFile {}", docFrequencyPathBase, docFrequencyFile);
				Path docFrequencyPath = new Path(docFrequencyPathBase, docFrequencyFile);
				docFrequencyChunks.add(docFrequencyPath);
				
				/*for (Pair<IntWritable, LongWritable> record
			            : new SequenceFileIterable<IntWritable, LongWritable>(docFrequencyPath, true, conf)) {
			        featureCount = Math.max(record.getFirst().get(), featureCount);
			    }*/
			}
			featureCount = 107623;
			featureCount++;
			
		    long vectorCount = Long.MAX_VALUE;
			/*Path tfDirPath = new Path(tfVectorsPaths + "/part-r-00000");
			int i = 0;
			for (Pair<Text, VectorWritable> record
		            : new SequenceFileIterable<Text, VectorWritable>(tfDirPath, true, conf)) {
				i++;
		    }
			if (i > 0) {
				vectorCount = i;
			}*/
		    vectorCount = 80000;
			//read docFrequencyFile to get featureCount and vectorCount
			Long[] counts = { featureCount, vectorCount };
			log.info("featureCount {}, vectorCount------------------ {}", featureCount, vectorCount);
			docFrequenciesFeatures = new Pair<Long[], List<Path>>(counts, docFrequencyChunks);
		}

      long maxDF = maxDFPercent; //if we are pruning by std dev, then this will get changed
      if (shouldPrune) {
        long vectorCount = docFrequenciesFeatures.getFirst()[1];
        if (maxDFSigma >= 0.0) {
          Path dfDir = new Path(outputDir, TFIDFConverter.WORDCOUNT_OUTPUT_FOLDER);
          Path stdCalcDir = new Path(outputDir, HighDFWordsPruner.STD_CALC_DIR);

          // Calculate the standard deviation
          double stdDev = BasicStats.stdDevForGivenMean(dfDir, stdCalcDir, 0.0, conf);
          maxDF = (int) (100.0 * maxDFSigma * stdDev / vectorCount);
        }

        long maxDFThreshold = (long) (vectorCount * (maxDF / 100.0f));

        // Prune the term frequency vectors
        Path tfDir = new Path(outputDir, tfDirName);
        Path prunedTFDir = new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER);
        Path prunedPartialTFDir =
                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER + "-partial");
        log.info("Pruning");
        if (processIdf == 1 || processIdf == 3) {
          HighDFWordsPruner.pruneVectors(tfDir,
                  prunedTFDir,
                  prunedPartialTFDir,
                  maxDFThreshold,
                  minDf,
                  conf,
                  docFrequenciesFeatures,
                  -1.0f,
                  false,
                  reduceTasks);
        } else {
          HighDFWordsPruner.pruneVectors(tfDir,
                  prunedTFDir,
                  prunedPartialTFDir,
                  maxDFThreshold,
                  minDf,
                  conf,
                  docFrequenciesFeatures,
                  norm,
                  logNormalize,
                  reduceTasks);
        }
        HadoopUtil.delete(new Configuration(conf), tfDir);
      }
      if (processIdf == 1 || processIdf == 3) {
        TFIDFConverter.processTfIdf(
                new Path(outputDir, DictionaryVectorizer.DOCUMENT_VECTOR_OUTPUT_FOLDER),
                outputDir, conf, docFrequenciesFeatures, minDf, maxDF, norm, logNormalize,
                sequentialAccessOutput, namedVectors, reduceTasks);
      }
    } catch (OptionException e) {
      log.error("Exception", e);
      CommandLineUtil.printHelp(group);
    }
    return 0;
  }

}
