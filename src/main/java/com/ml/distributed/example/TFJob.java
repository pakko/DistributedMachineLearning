package com.ml.distributed.example;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import com.ml.distributed.mapreduce.Collector;
import com.ml.distributed.mapreduce.Job;
import com.ml.distributed.mapreduce.Mapper;
import com.ml.distributed.mapreduce.Reducer;

public class TFJob {

	public static class Map implements Mapper<Object, String, String, Integer> {

		@Override
		public void map(Object o, String text,
				Collector<String, Integer> collector) {
			StringTokenizer tokenizer = new StringTokenizer(text);
			while (tokenizer.hasMoreTokens()) {
				String word = tokenizer.nextToken();
				collector.collect(word, 1);
			}

		}

	}

	public static class Reduce implements
			Reducer<String, Integer, String, Integer> {

		@Override
		public void reduce(String word, Iterator<Integer> ints,
				Collector<String, Integer> collector) {
			int sum = 0;
			while (ints.hasNext()) {
				sum += ints.next();
			}

			collector.collect(word, sum);
		}

	}

	public static void main(String[] args) {

		Job job = new Job();
		job.setMapperClass(Map.class);
		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		job.setInput(null, getTexts());
		job.run();
		Collector result = job.getOutput();

		System.out.println(result);
	}

	private static List<String> getTexts() {
		List<String> texts = new ArrayList<String>();
		texts.add("Hello World Bye World");
		texts.add("Hello Hadoop GoodBye Hadoop");
		return texts;
	}

	private static String getText() {
		return "Hello World Bye World Hello Hadoop GoodBye Hadoop";
	}
}
