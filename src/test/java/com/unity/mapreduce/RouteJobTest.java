package com.unity.mapreduce;


import java.io.IOException;
import java.util.ArrayList;

import junit.framework.Assert;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.unity.mapreduce.RouteMapper;
import com.unity.mapreduce.RouteMapper.COUNTERS;

public class RouteJobTest {
	MapDriver<LongWritable, Text, Text, IntWritable> mapDriver; 
	ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapreduceDriver;
	
	@Before
	public void setUp() {
		RouteMapper mapper = new RouteMapper();
		
		mapDriver = new MapDriver(mapper);
	}
	
	@Test
	public void testMapperOK() throws IOException {
		mapDriver.withInput(new LongWritable(), 
				new Text("2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,7,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA"));
		mapDriver.withOutput(new Text("ATL-PHX"), new IntWritable(7));
		mapDriver.runTest();
		Assert.assertEquals("Checking INVALID counter value is 0", mapDriver.getCounters().findCounter(COUNTERS.INVALID).getValue(), 0);
	}
	
	@Test
	public void testMapperNOTOK() throws IOException {
		mapDriver.withInput(new LongWritable(), 
				new Text("2000,1,28,5,1647,1647,1906,1859,HP,154,N808AW,259,252,233,NA,0,ATL,PHX,1587,15,11,0,NA,0,NA,NA,NA,NA,NA"));
		mapDriver.runTest();
		Assert.assertEquals("Checking INVALID counter value is 1", mapDriver.getCounters().findCounter(COUNTERS.INVALID).getValue(), 1);
	}

}