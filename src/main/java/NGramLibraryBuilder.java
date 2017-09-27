import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NGramLibraryBuilder {
	public static class NGramMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		int noGram;
		@Override
		public void setup(Context context) {
			// how to get n-gram from command line?
			Configuration configuration = context.getConfiguration();
			// If we get nothing from command line as "noGram", the default value is 5.
			noGram = configuration.getInt("noGram", 5);
		}

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Because in Driver.java, we have ' conf1.set("textinputformat.record.delimiter", ".") ',
			// So we read data by sentence instead of by line.
			String sentence = value.toString();
			sentence = sentence.trim().toLowerCase();
			// how to remove useless elements?
			sentence.replaceAll("[^a-z]", " ");

			// how to separate word by space?
			// split by ' ', '\t', ..., etc
			String[] words = sentence.split("\\s+");
			// Get rid of 1-gram
			if (words.length < 2) {
				return;
			}

			// how to build n-gram based on array of words?
			for (int i = 0; i < words.length; i++) {
				StringBuilder sb = new StringBuilder();
				sb.append(words[i]);
				for (int j = 1; i + j < words.length && j < noGram; j++) {
					sb.append(" ");
					sb.append(words[i + j]);
					context.write(new Text(sb.toString().trim()), new IntWritable(1));
				}
			}
		}
	}

	public static class NGramReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {

			// how to sum up the total count for each n-gram?
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

}