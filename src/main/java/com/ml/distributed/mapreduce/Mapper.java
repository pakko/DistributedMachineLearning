package com.ml.distributed.mapreduce;

public interface Mapper<K1, V1, K2, V2> {

	public void map(K1 k1, V1 v1, Collector<K2, V2> outputCollector);

}
