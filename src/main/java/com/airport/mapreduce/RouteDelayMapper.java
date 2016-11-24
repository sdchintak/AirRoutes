package com.airport.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RouteDelayMapper extends Mapper<LongWritable, Text, IntWritable, RouteDelay>{
	static final IntWritable ONE = new IntWritable(1);
	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object, java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String data = value.toString();
		String[] dataValues = data.split("\t");
		RouteDelay rd = new RouteDelay();
		rd.setRoute(new Text(dataValues[0]));
		rd.setDelay(new DoubleWritable(Double.parseDouble(dataValues[1])));
		context.write(ONE, new RouteDelay(dataValues[0], Double.parseDouble(dataValues[1])));
	}
	
}
