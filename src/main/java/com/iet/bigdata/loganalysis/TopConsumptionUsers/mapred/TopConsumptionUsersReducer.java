package com.iet.bigdata.loganalysis.TopConsumptionUsers.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iet.bigdata.loganalysis.TopConsumptionUsers.HBaseOperations;
import com.iet.bigdata.loganalysis.TopConsumptionUsers.TopConsumptionUsers;


public class TopConsumptionUsersReducer extends
		Reducer<Text, LongWritable, Text, Text> {

	private Text result = new Text();
	long sum = 0;
	int date=0;
	String user=null;
	TopConsumptionUsers top=new TopConsumptionUsers();
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			{
		sum = 0;
		for (LongWritable val : values) {
			sum += (long)val.get();
		}

		date=Integer.parseInt(key.toString().substring(0, 6));
		user=key.toString().substring(6);
		
		top.setDate(date);
		top.setUser(String.format("%-20s",user));
		top.setValue(sum);
		try {
			HBaseOperations.addRecord(HBaseOperations.getTable(), top.packKey());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
