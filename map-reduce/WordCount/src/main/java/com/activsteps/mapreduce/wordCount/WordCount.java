package com.activsteps.mapreduce.wordCount;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
    public static void main(String[] args) throws Exception {        

        String input = "input/test.txt";
        String output = "out";

        // Create a new job
        Job job = new Job();

        // Set job name to locate it in the distributed environment
        job.setJarByClass(WordCount.class);
        job.setJobName("Word Count");

        // Set input and output Path, note that we use the default input format
        // which is TextInputFormat (each record is a line of input)
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

       job.setMapperClass(WordCountMapper.class);
       job.setReducerClass(WordCountReducer.class);
        // Set Output key and value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}