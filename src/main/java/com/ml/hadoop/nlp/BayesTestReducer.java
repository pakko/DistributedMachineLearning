package com.ml.hadoop.nlp;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class BayesTestReducer extends
		Reducer<WritableComparable<?>, VectorWritable, WritableComparable<?>, DoubleWritable> {

	@Override
	protected void reduce(WritableComparable<?> key,
			Iterable<VectorWritable> values, Context ctx) throws IOException,
			InterruptedException {
		Vector vector = null;
		double bestScore = Long.MIN_VALUE;
		for (VectorWritable value : values) {
			vector = value.get();
			if (vector.maxValue() > bestScore) {
				bestScore = vector.maxValue();
			}
		}
		ctx.write(key, new DoubleWritable(bestScore));
	}
}