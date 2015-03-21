package com.iet.bigdata.markov2.prediction.mapred;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MarkovIIMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

		// The value is 1 line of the log.
		// Create following string here:
		// user_id , current_url , next_url
		word.set(value);
		context.write(word, one);

	}
}
