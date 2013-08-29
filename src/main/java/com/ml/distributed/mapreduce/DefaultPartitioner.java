package com.ml.distributed.mapreduce;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DefaultPartitioner<K, V> implements Partitioner<K, V> {

	@Override
	public List<Collector<K, V>> getPartition(Collector<K, V> collector) {

		List<Collector<K, V>> list = new ArrayList<Collector<K, V>>();

		Map<K, List<V>> map = collector.toMap();

		Set<Entry<K, List<V>>> set = map.entrySet();
		for (Iterator<Entry<K, List<V>>> it = set.iterator(); it.hasNext();) {
			Collector<K, V> c = new Collector<K, V>();
			Entry<K, List<V>> entry = it.next();
			K key = entry.getKey();
			List<V> value = entry.getValue();

			for (V v : value) {
				c.collect(key, v);
			}
			list.add(c);
		}

		return list;
	}
}
