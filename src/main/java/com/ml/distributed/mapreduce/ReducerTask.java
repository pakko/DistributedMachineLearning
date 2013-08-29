package com.ml.distributed.mapreduce;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class ReducerTask<K1, V1, K2, V2> {

	//input collector
	private Collector<K1, V1> collector;

	private Class<? extends Reducer<K1, V1, K2, V2>> reducerClass;

	//output collector
	private Collector<K2, V2> outputCollector = new Collector<K2, V2>();

	public void setInput(Collector<K1, V1> collector) {
		this.collector = collector;

	}

	public void setReducerClass(
			Class<? extends Reducer<K1, V1, K2, V2>> reducerClass) {
		this.reducerClass = reducerClass;

	}

	public void runTask() {

		Collector<K2, V2> outputCollector = new Collector<K2, V2>();

		Reducer<K1, V1, K2, V2> reducer = Utils.newInstance(reducerClass);

		Map<K1, List<V1>> map = collector.toMap();

		Set<Entry<K1, List<V1>>> set = map.entrySet();

		for (Entry<K1, List<V1>> entry : set) {

			reducer.reduce(entry.getKey(), entry.getValue().iterator(),
					outputCollector);
		}

		setOutputCollector(outputCollector);

	}

	public Collector<K2, V2> getOutput() {
		return outputCollector;

	}

	public void setOutputCollector(Collector<K2, V2> outputCollector) {
		this.outputCollector = outputCollector;
	}
}
