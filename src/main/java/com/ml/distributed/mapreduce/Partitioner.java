package com.ml.distributed.mapreduce;

import java.util.List;

public interface Partitioner<K, V> {

	public List<Collector<K, V>> getPartition(Collector<K, V> collector);

}
