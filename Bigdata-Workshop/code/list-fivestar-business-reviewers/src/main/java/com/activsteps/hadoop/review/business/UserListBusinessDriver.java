package com.activsteps.hadoop.review.business;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.activsteps.hadoop.utils.YelpDataInputFormat;

public class UserListBusinessDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		if (args.length < 3) {
			System.err.println("Usage: " + "UserCountReviewBusinessDriver"
					+ "<Business data>  <Review data> <out>");
			System.exit(2);
		}
		
		int exitCode = ToolRunner.run(new UserListBusinessDriver(),
				args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		Job job = new Job(getConf());
		job.setJarByClass(UserListBusinessDriver.class);

		Path out = new Path(args[2]);
		out.getFileSystem(getConf()).delete(out, true);

		MultipleInputs.addInputPath(job, new Path(args[0]),
				YelpDataInputFormat.class, BusinessMapper.class);
		
		MultipleInputs.addInputPath(job, new Path(args[1]),
				YelpDataInputFormat.class, ReviewMapper.class);

		job.setReducerClass(BusinessUserReducer.class);

		FileOutputFormat.setOutputPath(job, new Path(args[2]));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.waitForCompletion(true);
		return 0;
	}

}
