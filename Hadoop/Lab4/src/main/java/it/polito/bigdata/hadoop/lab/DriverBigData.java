package it.polito.bigdata.hadoop.lab;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import it.polito.bigdata.hadoop.lab.DriverBigData;
import it.polito.bigdata.hadoop.lab.MapperBigData1;
import it.polito.bigdata.hadoop.lab.MapperBigData2;
import it.polito.bigdata.hadoop.lab.ReducerBigData1;
import it.polito.bigdata.hadoop.lab.ReducerBigData2;

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {

		int exitCode;

		Configuration conf = this.getConf();

		// Define a new job
		Job job = Job.getInstance(conf);

		// Assign a name to the job
		job.setJobName("Lab#4 - Ex.1 - step 1");

		/*
		 * ********************************************************* 
		 * Fill out the missing parts/update the content of this method
		 ************************************************************
		*/

		/*
			NOTES: you can assume that the number of ratings per user is “small”, i.e., given an
			arbitrary user, his/her list of ratings can be stored in a local Java variable.

			DATA: ..., ProductId, UserId, ..., Score, ...

			First, I need to make the average per user: 
			1. Mapper1 must emit key:user(Text), and value:product+","+score(Text) as value
				* there is no clear Touple pre-made class so I will use string concats to replace tuples logically
				* avg and std are not associative (the mean of the means is not the global mean), so a combiner is not the right choice if we operate directly with the mean
			2. Reducer1 takes all product scores by a given user, parse them as floats and normalize them (mean and std)
			3. Reducer1 writes key:product(Text) value:normalized_score(FloatWritable)) in file (user can be dropped)

			Then I need to do the average for each product
			1. Mapper2 emits key:product(Text), value:score(FloatWritable) 
			2. Reducer2 takes all scores and averages them
			3. Reducer2 writes key:product(Text) value:avg_normalized_score(FloatWritable) in file
		*/

		Path inputPath;
		Path outputDir;
		Path outputDir2;
		int numberOfReducersJob1;
		int numberOfReducersJob2;

		// Parse the parameters for the set up of the first job
		numberOfReducersJob1 = Integer.parseInt(args[0]);
		numberOfReducersJob2 = 2;
		inputPath = new Path(args[1]);
		outputDir = new Path(args[2]);
		outputDir2 = new Path(args[3]);


		// Set the path of the input file/folder for this first job
		FileInputFormat.addInputPath(job, inputPath);

		// Set the path of the output folder for this job
		FileOutputFormat.setOutputPath(job, outputDir);

		// Specify the class of the Driver for this job
		job.setJarByClass(DriverBigData.class);

		// Set job input format
		job.setInputFormatClass(TextInputFormat.class);

		// Set job output format
		job.setOutputFormatClass(TextOutputFormat.class);

		// Set map class
		job.setMapperClass(MapperBigData1.class);

		// Set map output key and value classes
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		// Set reduce class
		job.setReducerClass(ReducerBigData1.class);

		// Set reduce output key and value classes
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		// Set number of reducers
		job.setNumReduceTasks(numberOfReducersJob1);

		// Execute the first job and wait for completion
		if (job.waitForCompletion(true) == true) {
			// Set up the second job
			Job job2 = Job.getInstance(conf);

			// Assign a name to the second job
			job2.setJobName("Lab#3 - Ex.1 - step 2");

			/* */
			// Change the following part of the code

			// Set path of the input file/folder for this second job
			// The output of the first job is the input of this second job    
			FileInputFormat.addInputPath(job2, outputDir);

			// Set path of the output folder for this job
			FileOutputFormat.setOutputPath(job2, outputDir2);

			// Specify the class of the Driver for this job
			job2.setJarByClass(DriverBigData.class);

			// Set job input format
			job2.setInputFormatClass(TextInputFormat.class);

			// Set job output format
			job2.setOutputFormatClass(TextOutputFormat.class);

			// Set map class
			job2.setMapperClass(MapperBigData2.class);

			// Set map output key and value classes
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(FloatWritable.class);

			// Set reduce class
			job2.setReducerClass(ReducerBigData2.class);

			// Set reduce output key and value classes
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(FloatWritable.class);

			// Set the number of reducers of the second job 
			job2.setNumReduceTasks(numberOfReducersJob2);

			// Execute the second job and wait for completion
			if (job2.waitForCompletion(true) == true)
				exitCode = 0;
			else
				exitCode = 1;
		} else
			exitCode = 1;

		return exitCode;

	}

	/**
	 * Main of the driver
	 */

	public static void main(String args[]) throws Exception {
		// Exploit the ToolRunner class to "configure" and run the Hadoop
		// application
		int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

		System.exit(res);
	}
}
