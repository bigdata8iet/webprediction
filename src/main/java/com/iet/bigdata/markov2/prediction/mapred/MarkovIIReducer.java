package com.iet.bigdata.markov2.prediction.mapred;

import java.io.IOException;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.iet.bigdata.markov2.prediction.MarkovIIHBaseOperations;

public class MarkovIIReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();
	byte rowkey[] = null;

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {

		String keyArr[] = key.toString().split(",");

		byte sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		// Create the byte[] key from keyArr[0] and keyArr[1]

		rowkey = Bytes.toBytes(keyArr[0] + keyArr[2] + keyArr[1] + keyArr[3]);
		// Create hbase put object with above key
		// Add Qualifier as keyArr[2] and sum as value
		//nextUrl = Bytes.toBytes();
		// save in hbase.
		try {
			MarkovIIHBaseOperations.addRecord(MarkovIIHBaseOperations.getTable(),
					 rowkey, sum);

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		result.set(sum);
		context.write(key, result);
	}

}
