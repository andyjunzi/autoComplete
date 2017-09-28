import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;


public class Driver {

	public static void main(String[] args) throws ClassNotFoundException, IOException, InterruptedException {

		String inputDir = args[0];
		String nGramLib = args[1];
		String numberOfNGram = args[2];
		// the word with frequency under threshold will be discarded
		String threshold = args[3];
		String numberOfFollowingWords = args[4];

		// job1
		Configuration conf1 = new Configuration();

		// how to customize separator?
		// Define the job to read data by sentence
		conf1.set("textinputformat.record.delimiter", ".");	// built-in parameter
		conf1.set("noGram", numberOfNGram);						// custom parameter

		Job job1 = Job.getInstance(conf1);
		job1.setJobName("NGram");
		job1.setJarByClass(Driver.class);
		
		job1.setMapperClass(NGramLibraryBuilder.NGramMapper.class);
		job1.setReducerClass(NGramLibraryBuilder.NGramReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(TextInputFormat.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		
		TextInputFormat.setInputPaths(job1, new Path(inputDir));
		TextOutputFormat.setOutputPath(job1, new Path(nGramLib));
		job1.waitForCompletion(true);
		
		// how to connect two jobs?
		// last output is second input
		
		// 2nd job
		Configuration conf2 = new Configuration();
		conf2.set("threshold", threshold);
		conf2.set("n", numberOfFollowingWords);
		
		DBConfiguration.configureDB(conf2, 
				"com.mysql.jdbc.Driver",
				"jdbc:mysql://98.31.47.173:8889/test",
				"root",
				"root");
		
		Job job2 = Job.getInstance(conf2);
		job2.setJobName("Model");
		job2.setJarByClass(Driver.class);

		// How to add external dependency to current project?
        /*
		  1. upload dependency to hdfs
		  2. use this "addArchiveToClassPath" method to define the dependency path on hdfs
		 */
		job2.addArchiveToClassPath(new Path("/mysql/mysql-connector-java-5.1.39-bin.jar"));

		job2.setMapperClass(LanguageModel.Map.class);
		job2.setReducerClass(LanguageModel.Reduce.class);

		// Why do we add both map outputKey and outputValue?
		// Because map output key and value are inconsistent with reducer output key and value
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(DBOutputWritable.class);
		job2.setOutputValueClass(NullWritable.class);
		
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(DBOutputFormat.class);

		// use DBOutputformat to define the table name and columns
		DBOutputFormat.setOutput(job2, "output", new String[] {"starting_phrase", "following_word", "count"});

		TextInputFormat.setInputPaths(job2, args[1]);
		job2.waitForCompletion(true);
	}

}
