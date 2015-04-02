package com.iet.bigdata.loganalysis.DomainConsumption.mapred;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DomainConsumptionMapper1 extends Mapper<Object, Text, Text, LongWritable> {

	private static LongWritable usage = new LongWritable();
	private Text word = new Text();
	private String arr[]=null;
	Date date=new Date();
	Calendar cal = Calendar.getInstance();
	private int year;

	public void map(Object key, Text value, Context context)
			{

		// The value is 1 line of the log.
		// Create following string here:
		// key<year,month,user,domain> value<usage>
		
		arr=value.toString().split(" ");
		
		if(!arr[7].equalsIgnoreCase("-"))
		{		
			arr[0]=arr[0].substring(0, arr[0].indexOf(".")) + arr[0].substring(arr[0].indexOf(".")+1);
			date.setTime(Long.parseLong(arr[0]));
	        cal.setTime(date);
	        year = cal.get(Calendar.YEAR)*100+(cal.get(Calendar.MONTH)+1);
			
			arr[6] = arr[6].replaceFirst("(?:https://)", "")
					.replaceFirst("(?:http://)", "");

			if(arr[6].contains("/"))
				arr[6]=arr[6].substring(0,arr[6].indexOf("/"));
			
			arr[6] = arr[6].replace(".com", "")
					.replace(".net", "")
					.replace(".co.in", "")
					.replace(".co", "")
					.replace(".in", "")
					.replace(".edu", "");
			
			
			if(arr[6].contains("."))
				arr[6]=arr[6].substring(arr[6].lastIndexOf(".")+1);
			if(arr[6].contains(":"))
				arr[6]=arr[6].substring(0,arr[6].indexOf(":"));
			
			
			usage.set(Long.parseLong(arr[4]));
			word.set(year+","+arr[6]);
			try {
				context.write(word, usage);
			} catch (IOException | InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
