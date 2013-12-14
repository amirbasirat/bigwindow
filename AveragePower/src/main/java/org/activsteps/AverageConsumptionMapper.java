package org.activsteps;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class AverageConsumptionMapper extends
		Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// check if record is valid
		if (PowerConsumptionRecord.isValidRecord(value.toString())) {
			// check if record has special charecters
			PowerConsumptionRecord record = new PowerConsumptionRecord(
					value.toString());

			double consumption = record.getSub_metering_1()
					+ record.getSub_metering_2() + record.getSub_metering_3();

			context.write(new IntWritable(record.getYear()),
					new DoubleWritable(consumption));

		}
	}
}
