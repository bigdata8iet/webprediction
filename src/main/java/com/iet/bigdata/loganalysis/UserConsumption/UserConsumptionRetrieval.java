package com.iet.bigdata.loganalysis.UserConsumption;

import java.util.Iterator;

import org.apache.hadoop.hbase.util.Bytes;

public class UserConsumptionRetrieval {

	public static void main(String[] args) throws Exception {
		UserConsumption dc=new UserConsumption();

		HBaseOperations.useTable();
		int count = 0;
		Iterator<UserConsumption> dataIterator = HBaseOperations.getAllRecords();
		while (dataIterator.hasNext()) {
			System.out.println(dataIterator.next());
			count++;
		}
		System.out.println(count + "Records Retrieved");
	}

}
