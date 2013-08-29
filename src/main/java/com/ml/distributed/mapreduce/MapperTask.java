package com.ml.distributed.mapreduce;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class MapperTask<K1, V1, K2, V2> {

	//input k,v
	private K1 k1;
	private List<V1> subV1s;

	private Class<? extends Mapper> mapperClass;
	private Class<? extends Reducer> combinerClass;
	
	//output collector
	private Collector<K2, V2> outputCollector;

	public void setInput(K1 k1, List<V1> subV1s) {
		this.k1 = k1;
		this.subV1s = subV1s;
	}

	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;

	}

	public void setCombinerClass(Class<? extends Reducer> combinerClass) {
		this.combinerClass = combinerClass;

	}

	public Collector<K2, V2> getOutput() {
		return outputCollector;
	}

	public void setOutputCollector(Collector<K2, V2> outputCollector) {
		this.outputCollector = outputCollector;
	}

	public void runTask() {

		Collector<K2, V2> outputCollector = new Collector<K2, V2>();

		Mapper mapper = Utils.newInstance(mapperClass);
		Reducer combiner = Utils.newInstance(combinerClass);

		Collector mapperOutputCollector = new Collector();
		for (V1 v1 : subV1s) {
			mapper.map(k1, v1, mapperOutputCollector);
		}

		Map map = mapperOutputCollector.toMap();
		Set set = map.entrySet();
		for (Iterator it = set.iterator(); it.hasNext();) {
			Entry entry = (Entry) it.next();
			Object key = entry.getKey();
			List value = (List) entry.getValue();

			combiner.reduce(key, value.iterator(), outputCollector);
		}

		setOutputCollector(outputCollector);

	}

}
