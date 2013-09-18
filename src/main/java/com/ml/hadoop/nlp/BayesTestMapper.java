package com.ml.hadoop.nlp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.mahout.classifier.naivebayes.AbstractNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.ComplementaryNaiveBayesClassifier;
import org.apache.mahout.classifier.naivebayes.NaiveBayesModel;
import org.apache.mahout.classifier.naivebayes.StandardNaiveBayesClassifier;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

import java.io.IOException;
import java.util.regex.Pattern;

/**
 * Run the input through the model and see if it matches.
 * <p/>
 * The output value is the generated label, the Pair is the expected label and true if they match:
 */
//public class BayesTestMapper extends Mapper<Text, VectorWritable, Text, VectorWritable> {
public class BayesTestMapper extends Mapper<Text, VectorWritable, Text, DoubleWritable> {
	
  private static final Pattern SLASH = Pattern.compile("/");

  private AbstractNaiveBayesClassifier classifier;

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    Path modelPath = HadoopUtil.getSingleCachedFile(conf);
    NaiveBayesModel model = NaiveBayesModel.materialize(modelPath, conf);
    boolean compl = Boolean.parseBoolean(conf.get(TestNaiveBayesDriver.COMPLEMENTARY));
    if (compl) {
      classifier = new ComplementaryNaiveBayesClassifier(model);
    } else {
      classifier = new StandardNaiveBayesClassifier(model);
    }
  }

  @Override
  protected void map(Text key, VectorWritable value, Context context) throws IOException, InterruptedException {
    Vector result = classifier.classifyFull(value.get());
    
    //the key is the expected value
    //context.write(new Text(SLASH.split(key.toString())[1]), new VectorWritable(result));
    context.write(new Text(SLASH.split(key.toString())[1]), new DoubleWritable(result.maxValue()));
  }
}
