package com.iet.bigdata.loganalysis.UserConsumption.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserConsumptionMapper2 extends Mapper<Object, Text, Text, Text> {

	Text rowKey=new Text();
	Text usage=new Text();
	
	public void map(Object key, Text value, Context context)
	{
		
		
		try {
			String[] arr = value.toString().split("\t");
			rowKey.set(arr[0]);
			usage.set(arr[1]);
			context.write(rowKey,usage);
		} catch (IOException | InterruptedException e) {
		
			e.printStackTrace();
		}
		
	}
}
