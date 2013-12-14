package org.activsteps;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Driver for calculating the Average Energy Consumption per year across all the households
 * 
 * input: Directory or files containing energy consumption data
 * output: output dir, will be created everytime
 * 
 */

public class AverageConsumptionDriver {

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: " + "AverageConsumptionDriver" + " <input> <output>");
			System.exit(2);
		}

		Job job = new Job(new Configuration());
		job.setJarByClass(AverageConsumptionDriver.class);

		job.setMapperClass(AverageConsumptionMapper.class);
		job.setReducerClass(AverageConsumptionReducer.class);

		job.setInputFormatClass(TextInputFormat.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(DoubleWritable.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));

		Path output = new Path(args[1]);
		FileSystem.get(new Configuration()).delete(output, true);
		FileOutputFormat.setOutputPath(job, output);

		int result = job.waitForCompletion(true) ? 0 : 1;
		System.exit(result);
	}

}
