package com.akrantha.nlp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.akrantha.bayes.BayesClassifier;
import com.akrantha.bayes.Classifier;

public class TweetSentiMapper extends Mapper<LongWritable, Text, Text, Text> {

	final Classifier<String, String> bayes = new BayesClassifier<String, String>();
	String[] positiveText = null;
	String[] negativeText = null;
	HashMap<String, String> userCache = new HashMap<String, String>();;

	public TweetSentiMapper() {

	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sentence = value.toString();

		String[] tokens = sentence.split("\\s");

		if (tokens[0].matches(".*\\d.*")) {
			context.write(new Text(sentence),
					new Text(bayes.classify(Arrays.asList(tokens))
							.getCategory()));

		}

	}

	protected void setup(Context context) throws IOException,
			InterruptedException {

		Path pt = new Path("hdfs://localhost:9000/user/qvantel/positive.txt");

		FileSystem fs = FileSystem.get(context.getConfiguration());
		BufferedReader br = new BufferedReader(new InputStreamReader(
				fs.open(pt)));
		try {
			String line;
			while ((line = br.readLine()) != null) {
				String[] rec = line.split(",");
				// userCache.put(rec[0], rec[1]);

				positiveText = line.split("\\s");
				bayes.learn("positive", Arrays.asList(positiveText));
			}
		} finally {
			br.close();
		}

		Path pathNeg = new Path(
				"hdfs://localhost:9000/user/qvantel/negative.txt");

		FileSystem fs2 = FileSystem.get(context.getConfiguration());
		BufferedReader br2 = new BufferedReader(new InputStreamReader(
				fs2.open(pathNeg)));
		try {
			String lineNeg;
			while ((lineNeg = br2.readLine()) != null) {
				String[] rec2 = lineNeg.split(",");
				// userCache.put(rec2[0], rec2[1]);
				negativeText = lineNeg.split("\\s");
				bayes.learn("negative", Arrays.asList(negativeText));
			}
		} finally {
			br.close();
		}

	}

}
