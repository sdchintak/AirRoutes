package com.unity.mapreduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RouteMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Log LOG = LogFactory.getLog(RouteMapper.class);
	// Counters 
	public enum COUNTERS {
		INVALID
	}

	/*
	 * @see org.apache.hadoop.mapreduce.Mapper#map(java.lang.Object,
	 * java.lang.Object, org.apache.hadoop.mapreduce.Mapper.Context)
	 * @param LongWritable
	 * @param Text
	 */
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// text line is split and the required fields are extracted
		try {
			String data = value.toString();
			String[] dataSplit = data.split(",");
			String origin = dataSplit[16];
			String destination = dataSplit[17];
			Integer delay = Integer.parseInt(dataSplit[14]);
			String route = origin + "-" + destination;
			// key is origin-destination and value is arrival delay
			context.write(new Text(route), new IntWritable(delay));
		} catch (Exception e) {
			context.getCounter(COUNTERS.INVALID).increment(1);
		}
	}



}
