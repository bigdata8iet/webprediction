package com.iet.bigdata.loganalysis.UserConsumption.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class UserConsumptionReducer1 extends
		Reducer<Text, LongWritable, Text, Text> {

	private Text result = new Text();
	long sum = 0;
	String domain=null;
	
	public void reduce(Text key, Iterable<LongWritable> values, Context context)
			{
		sum = 0;
		for (LongWritable val : values) {
			sum += (long)val.get();
		}

		domain=key.toString().substring(key.toString().lastIndexOf(",")+1);
		key.set(key.toString().substring(0,key.toString().lastIndexOf(",")));
		result.set(domain+","+sum);
		try {
			context.write(key, result);
		} catch (IOException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
