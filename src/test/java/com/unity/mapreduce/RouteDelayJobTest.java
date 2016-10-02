package com.unity.mapreduce;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.unity.mapreduce.RouteDelay;
import com.unity.mapreduce.RouteDelayMapper;


public class RouteDelayJobTest {
	
	MapDriver<LongWritable, Text, IntWritable, RouteDelay> mapDriver;

	@Before
	public void setUp() {
		RouteDelayMapper mapper = new RouteDelayMapper();
		
		mapDriver = new MapDriver(mapper);
	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("ABE-PIT	6.041666666666667"));
		mapDriver.withOutput(new IntWritable(1), new RouteDelay("ABE-PIT",6.041666666666667));
		mapDriver.runTest();
	}
	
}
