package org.activsteps;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageConsumptionReducer extends
		Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	@Override
	protected void reduce(IntWritable key, Iterable<DoubleWritable> values,
			Context context) throws IOException, InterruptedException {

		double totalVolume = 0.0;
		double totalRecords = 0;
		for (DoubleWritable value : values) {
			totalVolume += value.get();
			totalRecords++;
		}
		double mean = totalVolume / totalRecords;
		context.write(key, new DoubleWritable(mean));
	}

}
