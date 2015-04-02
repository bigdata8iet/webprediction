package com.iet.bigdata.loganalysis.UserConsumption.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iet.bigdata.loganalysis.UserConsumption.HBaseOperations;

public class UserConsumptionMapredMain {

	private static final String OUTPUT_PATH = "/mnt/data/workspace/prediction/test/intermediate_output";

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf, "UserConsumptionMapReduceJob1");

			job.setJarByClass(UserConsumptionMapredMain.class);
			job.setMapperClass(UserConsumptionMapper1.class);
			// job.setCombinerClass(UserConsumptionReducer1.class);
			job.setReducerClass(UserConsumptionReducer1.class);
			job.setNumReduceTasks(1);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(job, new Path(
					"/mnt/data/workspace/prediction/test/proxy_out"));
			FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));

			job.waitForCompletion(true);

			Configuration conf2 = new Configuration();
	
			Job job2 = Job.getInstance(conf2, "UserConsumptionMapReduceJob2");
	
			job2.setJarByClass(UserConsumptionMapredMain.class);
			job2.setMapperClass(UserConsumptionMapper2.class);
			//job2.setCombinerClass(UserConsumptionReducer2.class);
			job2.setReducerClass(UserConsumptionReducer2.class);
	
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(Text.class);
			HBaseOperations.useTable();
			FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
			FileOutputFormat
					.setOutputPath(
							job2,
							new Path(
									"/mnt/data/workspace/prediction/test/UserConsumption_Output"));
	
			System.out.println(job2.waitForCompletion(true) ? 0 : 1);
		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
