package com.iet.bigdata.loganalysis.DomainConsumption.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iet.bigdata.loganalysis.DomainConsumption.DomainConsumption;
import com.iet.bigdata.loganalysis.DomainConsumption.HBaseOperations;

public class DomainConsumptionReducer2 extends
		Reducer<Text, Text, Text, LongWritable> {

	private LongWritable result = new LongWritable();
	private Text row = new Text();
	long sum = 0, usage = 0;
	String arr[] = new String[2];
	DomainConsumption dc=new DomainConsumption();

	public void reduce(Text key, Iterable<Text> values, Context context){
		try 
		{
			dc.setDate(Integer.parseInt(key.toString()));
			for (Text val : values) {
				arr = val.toString().split(",");
				dc.setDomain(String.format("%-20s",arr[0]));
				
				row.set(key.toString() + "," + arr[0]);
				usage = Long.parseLong(arr[1]);
				dc.setValue(usage);
			
				HBaseOperations.addRecord(HBaseOperations.getTable(), dc.packKey(), dc.packUsage());
			
				result.set(usage);
				context.write(row, result);
				sum += usage;
			}
			
			dc.setDomain(String.format("%-20s","Total"));
			dc.setValue(sum);
			HBaseOperations.addRecord(HBaseOperations.getTable(), dc.packKey(),dc.packUsage());
		
			row.set(key.toString()+",Total");
			result.set(sum);
			context.write(row, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
