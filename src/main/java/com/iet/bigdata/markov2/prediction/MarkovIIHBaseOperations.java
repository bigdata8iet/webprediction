package com.iet.bigdata.markov2.prediction;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.iet.bigdata.markov1.prediction.MarkovI;

public class MarkovIIHBaseOperations extends Thread {
	private static Configuration conf = null;
	private static HColumnDescriptor family = null;
	static {
		conf = HBaseConfiguration.create();
	}

	public static void useTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		String tableName = "MarkovITable";
		byte[] families = Bytes.toBytes("MyFamily");
		if (admin.isTableAvailable(tableName.getBytes())) {
			System.out.println(tableName + " Table already exists!");
			System.out.println("Using Existing Table" + tableName);
		} else {
			HTableDescriptor tableDesc = new HTableDescriptor(
					tableName.getBytes());

			family = new HColumnDescriptor(families);
			tableDesc.addFamily(family);
			admin.createTable(tableDesc);
			System.out.println("Table Created " + tableName + " ok.");
		}

	}

	public static HTable getTable() throws Exception {

		String tableName = "MarkovITable";
		HTable table = new HTable(conf, tableName);
		return table;
	}

	public static void addRecord(HTable htable, byte[] qualifier,
			byte[] rowKey, byte count) throws Exception {
		try {
			int counter = 0;

			byte[] families = Bytes.toBytes("MyFamily");
			Put put = new Put(rowKey);
			put.add(families, qualifier, new byte[] { count });
			htable.put(put);
			counter++;
			System.out.println(counter + " Records Inserted To Table "
					+ htable.getName());

			/*
			 * if (counter % 1000 == 0) {
			 * 
			 * System.out.println(counter + " Records Inserted To Table " +
			 * htable.getName()); sleep((long) 500); System.gc();
			 * System.runFinalization(); }
			 */
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Iterator<MarkovI> getAllRecords() throws IOException {
		Iterator<MarkovI> dataIterator = new Iterator<MarkovI>() {
			String tableName = "MarkovITable";
			HTable table = new HTable(conf, tableName);
			ResultScanner scanner = table.getScanner(Bytes.toBytes("MyFamily"));
			Iterator<Result> resultIterator = scanner.iterator();

			public boolean hasNext() {
				return resultIterator.hasNext();
			}

			public void remove() {
				resultIterator.remove();
			}

			public MarkovI next() {
				Result result = resultIterator.next();
				byte[] value = null;
				byte[] qualifier = null;
				Map<byte[], byte[]> family = result.getFamilyMap(Bytes
						.toBytes("MyFamily"));
				for (Map.Entry<byte[], byte[]> entry : family.entrySet()) {
					qualifier = entry.getKey();
					value = entry.getValue();
				}

				byte[] key = result.getRow();

				MarkovI mk1 = MarkovI.unpack(key, qualifier);
				mk1.setCount(value[0]);
				return mk1;
			}
		};

		return dataIterator;
	}

}