package com.activsteps.mapreduce.invertedindex;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
public static final int RETAIlER_INDEX = 0;

 @Override
 public void map(LongWritable longWritable, Text text, Context context) throws IOException, InterruptedException {
  final String[] record = StringUtils.split(text.toString(), ",");
  final String retailer = record[RETAIlER_INDEX];
  for (int i = 1; i < record.length; i++) {
   final String keyword = record[i];

   context.write(new Text(keyword), new Text(retailer));
   }
  }
 }