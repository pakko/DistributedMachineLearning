package com.ml.distributed.mapreduce;

import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class Collector<K, V> {

	private List<Entry<K, V>> collector = new ArrayList<Entry<K, V>>();

	public void collect(K k, V v) {
		Entry<K, V> entry = new SimpleEntry<K, V>(k, v);
		collector.add(entry);

	}

	public void collectAll(Collector<K, V> collector) {
		if (collector == null)
			return;

		List<Entry<K, V>> list = collector.getCollector();
		for (Entry<K, V> entry : list) {
			collect(entry.getKey(), entry.getValue());
		}

	}

	public void collectMap(Map<K, V> map) {
		Set<Entry<K, V>> set = map.entrySet();
		for (Iterator<Entry<K, V>> it = set.iterator(); it.hasNext();) {
			Entry<K, V> entry = it.next();
			collect(entry.getKey(), entry.getValue());
		}

	}

	public List<Entry<K, V>> getCollector() {
		return collector;
	}

	public Map<K, List<V>> toMap() {
		Map<K, List<V>> resultMap = new HashMap<K, List<V>>();
		for (Entry<K, V> entry : collector) {
			K k = entry.getKey();
			V v = entry.getValue();

			if (!resultMap.containsKey(k)) {
				resultMap.put(k, new ArrayList<V>());
			}

			resultMap.get(k).add(v);

		}

		return resultMap;
	}

	@Override
	public String toString() {

		Iterator<Entry<K, V>> i = collector.iterator();
		if (!i.hasNext())
			return "{}";

		StringBuilder sb = new StringBuilder();
		sb.append('{');
		for (;;) {
			Entry<K, V> e = i.next();
			K key = e.getKey();
			V value = e.getValue();
			sb.append(key == this ? "(this Collector)" : key);
			sb.append('=');
			sb.append(value == this ? "(this Collector)" : value);
			if (!i.hasNext())
				return sb.append('}').toString();
			sb.append(", ");
		}

	}

}
