package com.iet.bigdata.loganalysis.DomainConsumption.mapred;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DomainConsumptionMapper2 extends Mapper<Object, Text, Text, Text> {
	
	public void map(Object key, Text value, Context context)
	{
		
		try {
			String[] arr = value.toString().split("\t");
			context.write(new Text(arr[0]), new Text(arr[1]));
		} catch (IOException | InterruptedException e) {
		
			e.printStackTrace();
		}
		
	}
}
