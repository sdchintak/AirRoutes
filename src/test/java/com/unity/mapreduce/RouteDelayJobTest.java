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
	ReduceDriver<IntWritable, RouteDelay, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, IntWritable, RouteDelay, Text, DoubleWritable> mapreduceDriver;


	@Before
	public void setUp() {
		RouteDelayMapper mapper = new RouteDelayMapper();
		RouteDelayReducer reducer= new RouteDelayReducer();

		mapDriver = new MapDriver(mapper);
		reduceDriver = new ReduceDriver(reducer);
		mapreduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);

	}
	
	@Test
	public void testMapper() throws IOException {
		mapDriver.withInput(new LongWritable(), new Text("ABE-PIT	6.041666666666667"));
		mapDriver.withOutput(new IntWritable(1), new RouteDelay("ABE-PIT",6.041666666666667));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer() throws IOException {
		ArrayList<RouteDelay> values = new ArrayList<RouteDelay>();
		values.add(new RouteDelay("ABQ-BWI",-6.5));
		values.add(new RouteDelay("ABE-PIT",6.041666666666667));
		
		reduceDriver.withInput(new IntWritable(1), values);
		
		reduceDriver.withOutput(new Text("ABE-PIT"), new DoubleWritable(6.041666666666667))
		.withOutput(new Text("ABQ-BWI"), new DoubleWritable(-6.5));

		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() throws IOException {
		mapreduceDriver.withInput(new LongWritable(), new Text("ABE-PIT	6.041666666666667"))
		.withInput(new LongWritable(), new Text("ABQ-BWI	-6.5"));
		
		mapreduceDriver.withOutput(new Text("ABE-PIT"), new DoubleWritable(6.041666666666667))
		.withOutput(new Text("ABQ-BWI"), new DoubleWritable(-6.5));
		
		mapreduceDriver.runTest();
	}
}
