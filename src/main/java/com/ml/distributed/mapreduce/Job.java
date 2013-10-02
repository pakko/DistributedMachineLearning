package com.ml.distributed.mapreduce;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;

public class Job {
	
	//input
	private Object k1;
	private Object v1;

	//output
	private Collector outputCollector;

	//map reduce class
	private Class<? extends Mapper> mapperClass;
	private Class<? extends Reducer> combinerClass;
	private Class<? extends Reducer> reducerClass;

	private Partitioner partitioner = new DefaultPartitioner();


	public void run() {

		Collector mulitTreadMapperTaskInputCollector = doSplit(k1, v1);// split
		List<Collector> collectors = doMulitTreadMapperTask(mulitTreadMapperTaskInputCollector);// mapper
		Collector unionCollector = union(collectors);// union
		List<Collector> multiTreadReducerTaskInputCollector = doPartitioner(unionCollector);// partition
		collectors = doMultiTreadReducerTask(multiTreadReducerTaskInputCollector);// reducer
		outputCollector = union(collectors);

	}

	//
	public Collector doSplit(Object k1, Object v1) {
		Collector collector = new Collector();
		List list = Utils.split(v1, 1);
		for (Iterator it = list.iterator(); it.hasNext();) {
			collector.collect(k1, (List) it.next());
		}
		return collector;
	}

	//
	public List<Collector> doMulitTreadMapperTask(Collector collector) {

		List<Collector> mapperOutputCollectors = new ArrayList<Collector>();

		List<Entry> list = collector.getCollector();

		ExecutorService executorService = Executors.newFixedThreadPool(list
				.size());

		List<FutureTask<Collector>> futureTasks = new ArrayList<FutureTask<Collector>>();

		for (final Entry entry : list) {
			FutureTask<Collector> futureTask = new FutureTask<Collector>(
					new Callable<Collector>() {

						@Override
						public Collector call() throws Exception {

							MapperTask mapperTask = new MapperTask();

							mapperTask.setInput(entry.getKey(),
									(List) entry.getValue());
							mapperTask.setMapperClass(mapperClass);
							mapperTask.setCombinerClass(combinerClass);
							mapperTask.runTask();
							return mapperTask.getOutput();

						}
					});

			futureTasks.add(futureTask);
			executorService.submit(futureTask);

		}

		for (FutureTask<Collector> futureTask : futureTasks) {
			try {
				mapperOutputCollectors.add(futureTask.get());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		executorService.shutdown();

		return mapperOutputCollectors;

	}

	private Collector union(List<Collector> collectors) {
		Collector collector = new Collector();
		for (Collector c : collectors) {
			collector.collectAll(c);
		}
		return collector;
	}

	private List<Collector> doPartitioner(Collector collector) {

		return partitioner.getPartition(collector);
	}

	private List<Collector> doMultiTreadReducerTask(List<Collector> collectors) {

		List<Collector> reducerOutputCollectors = new ArrayList<Collector>();

		ExecutorService executorService = Executors
				.newFixedThreadPool(collectors.size());

		List<FutureTask<Collector>> futureTasks = new ArrayList<FutureTask<Collector>>();

		for (final Collector collector : collectors) {
			FutureTask<Collector> futureTask = new FutureTask<Collector>(
					new Callable<Collector>() {

						@Override
						public Collector call() throws Exception {

							ReducerTask reducerTask = new ReducerTask();
							reducerTask.setInput(collector);
							reducerTask.setReducerClass(reducerClass);
							reducerTask.runTask();
							return reducerTask.getOutput();
						}
					});

			futureTasks.add(futureTask);
			executorService.submit(futureTask);

		}

		for (FutureTask<Collector> futureTask : futureTasks) {
			try {
				reducerOutputCollectors.add(futureTask.get());
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}

		executorService.shutdown();

		return reducerOutputCollectors;
	}
	

	public void setInput(Object k1, Object v1) {
		this.k1 = k1;
		this.v1 = v1;
	}

	public Collector getOutput() {
		return outputCollector;
	}

	public void setMapperClass(Class<? extends Mapper> mapperClass) {
		this.mapperClass = mapperClass;
	}

	public void setCombinerClass(Class<? extends Reducer> combinerClass) {
		this.combinerClass = combinerClass;
	}

	public void setReducerClass(Class<? extends Reducer> reducerClass) {
		this.reducerClass = reducerClass;
	}

	public void setPartitioner(Partitioner partitioner) {
		this.partitioner = partitioner;
	}
}