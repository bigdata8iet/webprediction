package com.iet.bigdata.markov2.prediction.mapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.iet.bigdata.markov2.prediction.MarkovIIHBaseOperations;

public class MarkovIIMapredMain {

	public static void main(String[] args) {
		Configuration conf = new Configuration();
		try {
			Job job = Job.getInstance(conf, "MarkovIIMapReduceJob");

			job.setJarByClass(MarkovIIMapredMain.class);
			job.setMapperClass(MarkovIIMapper.class);
			job.setCombinerClass(MarkovIIReducer.class);
			job.setReducerClass(MarkovIIReducer.class);
			job.setNumReduceTasks(1);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat
					.addInputPath(
							job,
							new Path(
									"/mnt/data/workspace/prediction/test/preprocess_markov2.txt"));
			FileOutputFormat.setOutputPath(job, new Path(
					"/mnt/data/workspace/prediction/test/MarkovII_Output"));

			MarkovIIHBaseOperations.useTable();
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		} catch (Exception e) {
			e.getStackTrace();
			e.getCause();
		}

	}

}
