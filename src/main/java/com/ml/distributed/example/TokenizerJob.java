package com.ml.distributed.example;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.chenlb.mmseg4j.analysis.MMSegAnalyzer;
import com.google.common.io.Closeables;
import com.ml.distributed.mapreduce.Collector;
import com.ml.distributed.mapreduce.Job;
import com.ml.distributed.mapreduce.Mapper;
import com.ml.distributed.mapreduce.Reducer;
import com.ml.hadoop.nlp.StopWordsHandler;

public class TokenizerJob {

	public static class Map implements Mapper<String, String, String, String> {
		
		private Analyzer analyzer = new MMSegAnalyzer();

		@Override
		public void map(String key, String value,
				Collector<String, String> collector) {

			try {
				TokenStream stream = analyzer.tokenStream(key.toString(), new StringReader(value.toString()));
				stream.reset();
				CharTermAttribute termAtt = stream.addAttribute(CharTermAttribute.class);
				stream.reset();
				while (stream.incrementToken()) {
					if (termAtt.length() > 0) {
						String token = new String(termAtt.buffer(), 0, termAtt.length());
						if (StopWordsHandler.isChineseStopWord(token) == false
								&& StopWordsHandler.isEnglishStopWord(token) == false) {
							collector.collect(key, token);
						}
					}
				}
				stream.end();
				Closeables.close(stream, true);

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public static class Reduce implements Reducer<String, Integer, String, Integer> {

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
