package com.unity.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobChainDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		Path inputPath = new Path(args[0]);
		Path outputPath = new Path(args[1]);
		Path intPath = new Path("intermediate");
		int code;
		
		// delete intermediate and output directories
		
		FileSystem local = FileSystem.getLocal(conf);
		local.delete(intPath, true);
		local.delete(outputPath, true);
		
		// JOB - 1
		
		Job job1 = new Job(conf, "job-1");
		
		job1.setMapperClass(RouteMapper.class);
		job1.setReducerClass(RouteReducer.class);
		
		FileInputFormat.addInputPath(job1, inputPath);
		FileOutputFormat.setOutputPath(job1, intPath);
		
		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		code = job1.waitForCompletion(true) ? 0 : 1;
		
		// JOB-2
		Job job2 = new Job(conf, "job-2");
		
		job2.setMapperClass(RouteDelayMapper.class);
		job2.setReducerClass(RouteDelayReducer.class);
		//job2.setNumReduceTasks(0);
		
		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(RouteDelay.class);
		
		FileInputFormat.addInputPath(job2, intPath);
		FileOutputFormat.setOutputPath(job2, outputPath);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		code = job2.waitForCompletion(true) ? 0 : 1;		
		
		return code;
		
	}
	
	public static void main(String[] args) throws Exception {
		int code = ToolRunner.run(new Configuration(), new JobChainDriver(), args);
		System.exit(code);
	}

}
