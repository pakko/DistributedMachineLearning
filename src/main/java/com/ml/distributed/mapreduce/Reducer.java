package com.ml.distributed.mapreduce;

import java.util.Iterator;

public interface Reducer<K1, V1, K2, V2> {

	public void reduce(K1 k1, Iterator<V1> it, Collector<K2, V2> outputCollector);

}
