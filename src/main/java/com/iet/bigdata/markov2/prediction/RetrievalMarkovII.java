package com.iet.bigdata.markov2.prediction;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

import com.iet.bigdata.markov1.prediction.MarkovI;

public class RetrievalMarkovII {

	public static void main(String[] args) throws Exception {
		MarkovI mk = new MarkovI();
		byte[] family = Bytes.toBytes("MyFamily");
		byte[] qualifier = Bytes.toBytes("Qualifier");

		MarkovIIHBaseOperations.useTable();
		int count = 0;
		Iterator<MarkovI> dataIterator = MarkovIIHBaseOperations
				.getAllRecords();
		while (dataIterator.hasNext()) {
			System.out.println(dataIterator.next());
			count++;
		}
		System.out.println(count + "Records Retrieved");
	}

}
