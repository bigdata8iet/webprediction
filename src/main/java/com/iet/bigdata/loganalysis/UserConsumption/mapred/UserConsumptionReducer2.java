package com.iet.bigdata.loganalysis.UserConsumption.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iet.bigdata.loganalysis.UserConsumption.HBaseOperations;
import com.iet.bigdata.loganalysis.UserConsumption.UserConsumption;

public class UserConsumptionReducer2 extends
		Reducer<Text, Text, Text, LongWritable> {

	private LongWritable result = new LongWritable();
	private Text row = new Text();
	long sum = 0, usage = 0;
	String arr[] = new String[2];
	String arr1[] = new String[2];
	UserConsumption uc=new UserConsumption();
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		try 
		{
			arr1=key.toString().split(",");
			uc.setDate(Integer.parseInt(arr1[0]));
			uc.setUser(String.format("%-20s", arr1[1]));
			sum=0;
			for (Text val : values) {
				arr = val.toString().split(",");
				uc.setDomain(String.format("%-20s",arr[0]));
				
				row.set(key.toString() + "," + arr[0]);
				usage = Long.parseLong(arr[1]);
				uc.setValue(usage);
			
				HBaseOperations.addRecord(HBaseOperations.getTable(), uc.packKey(), uc.packUsage());
			
				result.set(usage);
				context.write(row, result);
				sum += usage;
			}
			
			uc.setDomain(String.format("%-20s","Total"));
			uc.setValue(sum);
			HBaseOperations.addRecord(HBaseOperations.getTable(), uc.packKey(),uc.packUsage());
		
			row.set(key.toString()+",Total");
			result.set(sum);
			context.write(row, result);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
