package com.ml.hadoop.nlp;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.AbstractJob;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.commandline.DefaultOptionCreator;
import org.apache.mahout.math.DenseVector;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 * Test the (Complementary) Naive Bayes model that was built during training by
 * running the iterating the test set and comparing it to the model
 */
public class TestNaiveBayesDriver extends AbstractJob {

	public static final String COMPLEMENTARY = "class"; // b for bayes, c for
														// complementary
	private static final Pattern SLASH = Pattern.compile("/");

	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new TestNaiveBayesDriver(), args);
	}

	@Override
	public int run(String[] args) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(addOption(DefaultOptionCreator.overwriteOption().create()));
		addOption("model", "m", "The path to the model built during training",
				true);
		addOption(buildOption("testComplementary", "c", "test complementary?",
				false, false, String.valueOf(false)));
		addOption(buildOption("runSequential", "seq", "run sequential?", false,
				false, String.valueOf(false)));
		addOption("labelIndex", "l",
				"The path to the location of the label index", true);
		Map<String, List<String>> parsedArgs = parseArguments(args);
		if (parsedArgs == null) {
			return -1;
		}
		if (hasOption(DefaultOptionCreator.OVERWRITE_OPTION)) {
			HadoopUtil.delete(getConf(), getOutputPath());
		}

		boolean complementary = hasOption("testComplementary");
		boolean sequential = hasOption("runSequential");
		if (sequential) {
			FileSystem fs = FileSystem.get(getConf());
			NaiveBayesModel model = NaiveBayesModel.materialize(new Path(
					getOption("model")), getConf());
			AbstractNaiveBayesClassifier classifier;
			if (complementary) {
				classifier = new ComplementaryNaiveBayesClassifier(model);
			} else {
				classifier = new StandardNaiveBayesClassifier(model);
			}
			SequenceFile.Writer writer = new SequenceFile.Writer(fs, getConf(),
					getOutputPath(), Text.class, VectorWritable.class);
			Reader reader = new Reader(fs, getInputPath(), getConf());
			Text key = new Text();
			VectorWritable vw = new VectorWritable();
			while (reader.next(key, vw)) {
				Vector result = classifier.classifyFull(vw.get());
				Vector r = new DenseVector(result.size());
				r.setQuick(result.maxValueIndex(), result.maxValue());
				writer.append(new Text(SLASH.split(key.toString())[1]), new VectorWritable(r));
			}
			writer.close();
			reader.close();
		} else {
			boolean succeeded = runMapReduce(parsedArgs);
			if (!succeeded) {
				return -1;
			}
		}
		return 0;
	}

	private boolean runMapReduce(Map<String, List<String>> parsedArgs)
			throws IOException, InterruptedException, ClassNotFoundException {
		Path model = new Path(getOption("model"));
		HadoopUtil.cacheFiles(model, getConf());
		// the output key is the expected value, the output value are the scores
		// for all the labels
		Job testJob = prepareJob(getInputPath(), getOutputPath(),
				SequenceFileInputFormat.class, BayesTestMapper.class,
				Text.class, VectorWritable.class,
				SequenceFileOutputFormat.class);
		// testJob.getConfiguration().set(LABEL_KEY, getOption("--labels"));
		boolean complementary = parsedArgs.containsKey("testComplementary");
		testJob.getConfiguration().set(COMPLEMENTARY,
				String.valueOf(complementary));
		return testJob.waitForCompletion(true);
	}

}
