package com.akrantha.hadoop.nlp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.akrantha.nlp.TweetSentiMapper;

public class CountUniqueTokensMain {

	public static void main(String args[]) throws Exception {
		Path inputpath = new Path(args[0]);
		Path outputpath = new Path(args[1]);
		Job job = new Job();
		job.setJarByClass(com.akrantha.hadoop.nlp.CountUniqueTokensMain.class);
		FileSystem.get(new Configuration()).delete(outputpath, true);
		
		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);
			
		// job.setMapperClass(TokenizerMapper.class);
		job.setMapperClass(TweetSentiMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
	}
}
