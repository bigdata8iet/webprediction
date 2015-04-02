package com.iet.bigdata.loganalysis.TopConsumptionUsers.mapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iet.bigdata.loganalysis.TopConsumptionUsers.HBaseOperations;

public class TopConsumptionUsersMapredMain {

	private static final String OUTPUT_PATH = "/mnt/data/workspace/prediction/test/intermediate_output";

	public static void main(String[] args) {

		Configuration conf = new Configuration();
		Job job;
		try {
			job = Job.getInstance(conf, "UserConsumptionMapReduceJob1");

			job.setJarByClass(TopConsumptionUsersMapredMain.class);
			job.setMapperClass(TopConsumptionUsersMapper.class);
			// job.setCombinerClass(UserConsumptionReducer1.class);
			job.setReducerClass(TopConsumptionUsersReducer.class);
			job.setNumReduceTasks(1);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileInputFormat.addInputPath(job, new Path(
					"/mnt/data/workspace/prediction/test/proxy_out"));
			FileOutputFormat.setOutputPath(job, new Path(
					"/mnt/data/workspace/prediction/test/TopConsumptionUsers"));
			
			HBaseOperations.useTable();
			job.waitForCompletion(true);

		} catch (IOException | ClassNotFoundException | InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
