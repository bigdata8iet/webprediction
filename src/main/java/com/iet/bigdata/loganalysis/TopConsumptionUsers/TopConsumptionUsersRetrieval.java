package com.iet.bigdata.loganalysis.TopConsumptionUsers;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

public class TopConsumptionUsersRetrieval {

	public static void main(String[] args) throws Exception {
		TopConsumptionUsers dc=new TopConsumptionUsers();

		HBaseOperations.useTable();
		int count = 0;
		Iterator<TopConsumptionUsers> dataIterator = HBaseOperations.getAllRecords();
		while (dataIterator.hasNext()) {
			System.out.println(dataIterator.next());
			count++;
		}
		System.out.println(count + "Records Retrieved");
	}

}
