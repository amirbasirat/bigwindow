package com.activsteps.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class InvertedIndex {

	private static final Log log = LogFactory.getLog(InvertedIndex.class);

	public static void main(String[] args) throws Exception {
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"/applicationContext.xml", InvertedIndex.class);
		log.info("InvertedIndex Application Running");
		context.registerShutdownHook();
	}

	public static class InvertedIndexMapper extends
			Mapper<LongWritable, Text, Text, Text> {
		public static final int RETAIlER_INDEX = 0;

		@Override
		public void map(LongWritable longWritable, Text text, Context context)
				throws IOException, InterruptedException {
			final String[] record = StringUtils.split(text.toString(), ",");
			final String retailer = record[RETAIlER_INDEX];
			for (int i = 1; i < record.length; i++) {
				final String keyword = record[i];

				context.write(new Text(keyword), new Text(retailer));
			}
		}
	}

	public static class InvertedIndexReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

}
