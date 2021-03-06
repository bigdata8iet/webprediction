package com.iet.bigdata.markov2.prediction;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

import com.iet.bigdata.markov1.prediction.MarkovI;

public class RetrievalMarkovII {

	public static void main(String[] args) throws Exception {
		MarkovIIHBaseOperations.useTable();
		int count = 0;
		Iterator<MarkovII> dataIterator = MarkovIIHBaseOperations.getAllRecords();
		while (dataIterator.hasNext()) {
			System.out.println(dataIterator.next());
			count++;
		}
		System.out.println(count + "Records Retrieved");
	}

}
