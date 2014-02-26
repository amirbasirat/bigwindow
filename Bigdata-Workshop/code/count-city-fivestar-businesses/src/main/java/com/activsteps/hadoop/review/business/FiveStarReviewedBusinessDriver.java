package com.activsteps.hadoop.review.business;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.activsteps.hadoop.utils.YelpDataInputFormat;

public class FiveStarReviewedBusinessDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: " + "FiveStarReviewedBusinessDriver"
					+ "<Business data> <out>");
			System.exit(2);
		}
		int exitCode = ToolRunner.run(new FiveStarReviewedBusinessDriver(),
				args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {

		Job job = new Job(getConf());
		job.setJarByClass(FiveStarReviewedBusinessDriver.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(job.getConfiguration()).delete(outputPath,
				true);

		job.setMapperClass(BusinessMapper.class);
		job.setReducerClass(FiveStarReviewedBusinessReducer.class);

		job.setInputFormatClass(YelpDataInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.setMapOutputValueClass(Text.class);
		job.setMapOutputKeyClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.waitForCompletion(true);
		return 0;
	}

}

