package com.iet.bigdata.loganalysis.DomainConsumption;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.iet.bigdata.loganalysis.UserConsumption.UserConsumption;


public class HBaseOperations extends Thread {

	static String tableName = "DomainConsumptionTable";
	static byte[] families = Bytes.toBytes("MyFamily");
	static byte[] consumption = Bytes.toBytes("Consumption");
	private static Configuration conf = null;
	private static HColumnDescriptor family = null;
	
	static int counter = 0;
	static {
		conf = HBaseConfiguration.create();
	}

	public static void useTable() throws Exception {
		HBaseAdmin admin = new HBaseAdmin(conf);
		
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
		HTable table = new HTable(conf, tableName);
		return table;
	}

	public static void addRecord(HTable htable,
			byte[] rowKey, byte[] usage) throws Exception {
		try {
			
			Put put = new Put(rowKey);
			put.add(families, consumption, usage);
			htable.put(put);
			counter++;
			System.out.println(counter + " Records Inserted To Table "
					+ htable.getName());
			
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static Iterator<DomainConsumption> getAllRecords() throws IOException {
		Iterator<DomainConsumption> dataIterator = new Iterator<DomainConsumption>() {
			
			HTable table = new HTable(conf, tableName);
			byte[] value = null;
			byte[] key=null;
			ResultScanner scanner = table.getScanner(families);
			Iterator<Result> resultIterator = scanner.iterator();

			public boolean hasNext() {
				return resultIterator.hasNext();
			}

			public void remove() {
				resultIterator.remove();
			}

			public DomainConsumption next() {
				DomainConsumption dc=new DomainConsumption();
				Result result = resultIterator.next();
				dc.unpackKey(result.getRow());
				dc.unpackUsage(result.getValue(families, consumption));
				return dc;
			}
		};

		return dataIterator;
	}

	public static Set<DomainConsumption> getSelectedInfo(byte[] start,
			byte[] end) throws Exception {
		
		Set<DomainConsumption> mk = new HashSet<DomainConsumption>();
		DomainConsumption element =new DomainConsumption();
		Scan scan = new Scan(start, end);
		HTable table = getTable();
		ResultScanner scanner = table.getScanner(scan);
		for (Result r : scanner) {
			
			element.unpackKey(r.getRow());
			element.unpackUsage(r.getValue(families,consumption));
			System.out.println(element);
			mk.add(element);
		}
		scanner.close();
		return mk;
	}

	
}