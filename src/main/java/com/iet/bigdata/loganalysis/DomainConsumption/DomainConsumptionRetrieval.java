package com.iet.bigdata.loganalysis.DomainConsumption;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

public class DomainConsumptionRetrieval {

	public static void main(String[] args) throws Exception {
		DomainConsumption dc=new DomainConsumption();

		HBaseOperations.useTable();
		int count = 0;
		Iterator<DomainConsumption> dataIterator = HBaseOperations.getAllRecords();
		while (dataIterator.hasNext()) {
			System.out.println(dataIterator.next());
			count++;
		}
		System.out.println(count + "Records Retrieved");
	}

}
