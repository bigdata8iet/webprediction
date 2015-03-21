package com.iet.bigdata.markov1.prediction.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iet.bigdata.markov1.prediction.MarkovIHBaseOperations;

public class MarkovIMapredMain {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MarkovIMapReduceJob");

		job.setJarByClass(MarkovIMapredMain.class);
		job.setMapperClass(MarkovIMapper.class);
		job.setCombinerClass(MarkovIReducer.class);
		job.setReducerClass(MarkovIReducer.class);
		job.setNumReduceTasks(0);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(
				"/mnt/data/workspace/webprediction/test/preprocess.txt"));
		FileOutputFormat.setOutputPath(job, new Path(
				"/mnt/data/workspace/webprediction/test/output2"));

		MarkovIHBaseOperations.useTable();
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
