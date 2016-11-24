package com.airport.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RouteDelayReducer extends Reducer<IntWritable, RouteDelay, Text, DoubleWritable>{

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(java.lang.Object, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<RouteDelay> values,Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		ArrayList<RouteDelay> valuesList = new ArrayList<RouteDelay>();
		for(RouteDelay value : values) {
			valuesList.add(new RouteDelay(value.getRoute(), value.getDelay()));
		}
		Collections.sort(valuesList, Collections.reverseOrder());
		
		// loopMax is created to restrict the top N items as N may be less than 100
		int loopMax;
		
		if(valuesList.size() < 100) {
			loopMax = valuesList.size();
		} else {
			loopMax = 100;
		}
		
		for(int i = 0; i < loopMax; i++) {
			context.write(valuesList.get(i).getRoute(), valuesList.get(i).getDelay());
		}
	}	
}
