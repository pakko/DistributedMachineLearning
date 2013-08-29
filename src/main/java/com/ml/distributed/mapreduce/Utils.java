package com.ml.distributed.mapreduce;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Utils {

	public static <T> T newInstance(Class<T> clazz) {
		try {
			return clazz.newInstance();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public static List split(Object original, final int maxListSize) {
		if (original instanceof List) {
			return split0((List) original, maxListSize);
		} else {
			List list = new ArrayList();
			list.add(original);
			return split0(list, maxListSize);
		}
	}

	private static <T> List<List<T>> split0(final List<T> original,
			final int maxListSize) {

		T[] elements = (T[]) original.toArray();
		int maxChunks = (int) Math.ceil(elements.length / (double) maxListSize);

		List<List<T>> lists = new ArrayList<List<T>>(maxChunks);
		for (int i = 0; i < maxChunks; i++) {
			int from = i * maxListSize;
			int to = Math.min(from + maxListSize, elements.length);
			T[] range = Arrays.copyOfRange(elements, from, to);

			lists.add(createSubList(range));
		}

		return lists;
	}

	private static <T> List<T> createSubList(final T[] elements) {
		List<T> list = new ArrayList<T>();
		list.addAll(Arrays.asList(elements));
		return list;
	}

}
