package com.unity.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RouteReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		// summing up the delay and counting number of values to calculate average
		Double delaySum = 0.0;
		Integer count = 0;
		for(IntWritable value : values) {
			delaySum = delaySum + value.get();
			count = count + 1;
		}
		
		context.write(key, new DoubleWritable(delaySum / count));
	}
	
}
