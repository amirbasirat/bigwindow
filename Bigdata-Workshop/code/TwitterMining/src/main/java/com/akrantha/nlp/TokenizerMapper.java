package com.akrantha.nlp;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;

import opennlp.tools.cmdline.PerformanceMonitor;
import opennlp.tools.postag.POSModel;
import opennlp.tools.postag.POSSample;
import opennlp.tools.postag.POSTaggerME;
import opennlp.tools.tokenize.Tokenizer;
import opennlp.tools.tokenize.TokenizerME;
import opennlp.tools.tokenize.TokenizerModel;
import opennlp.tools.tokenize.WhitespaceTokenizer;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TokenizerMapper extends
		Mapper<LongWritable, Text, Text, Text> {

	Tokenizer tokenizer;
	InputStream modelIn = null;
	TokenizerModel model;

	InputStream posModelIn = null;
	POSModel posModel;

	PerformanceMonitor perfMon;
	POSTaggerME tagger;

	public TokenizerMapper() {

	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String sentence = value.toString();
		int j = 0;
        String[] tokens = sentence.split(" ");
		String line = POSTag(sentence);
		context.write(new Text(tokens[0]), new Text(line));

		/*
		 * for (String t : tokenizer.tokenize(sentence)) {
		 * 
		 * }
		 */
	}

	protected void setup(Context context) throws IOException,
			InterruptedException {

		try {

			FileSystem fileSystem = FileSystem.get(context.getConfiguration());

			Path path = new Path("/bins/en-token.bin");

			modelIn = fileSystem.open(path);
			model = new TokenizerModel(modelIn);
			tokenizer = new TokenizerME(model);

			Path pospath = new Path("/bins/en-pos-maxent.bin");

			posModelIn = fileSystem.open(pospath);
			posModel = new POSModel(posModelIn);
			perfMon = new PerformanceMonitor(System.err, "sent");
			tagger = new POSTaggerME(posModel);

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (modelIn != null) {
				try {
					modelIn.close();
				} catch (IOException e) {
				}
			}
		}
	}

	public String[] tokenize(String s) {
		return tokenizer.tokenize(s);
	}

	public String POSTag(String input) throws IOException {

		ObjectStream<String> lineStream = new PlainTextByLineStream(
				new StringReader(input));
		POSSample sample = null;

		String line;
		while ((line = lineStream.read()) != null) {

			String whitespaceTokenizerLine[] = WhitespaceTokenizer.INSTANCE
					.tokenize(line);
			String[] tags = tagger.tag(whitespaceTokenizerLine);

			sample = new POSSample(whitespaceTokenizerLine, tags);

		}

		return sample.toString();
	}
}
