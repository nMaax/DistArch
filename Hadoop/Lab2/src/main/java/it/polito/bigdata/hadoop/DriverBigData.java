package it.polito.bigdata.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

enum COUNTERS {
  SELECTED_WORDS,
  UNSELECTED_WORDS
}

/**
 * MapReduce program
 */
public class DriverBigData extends Configured implements Tool {

  @Override
  public int run(String[] args) throws Exception {

	int exitCode = 0;

    // Handle args parameters
    Path inputPath;
    Path outputDir;
    int numberOfReducers;
    String filterPrefix;

	
	  // Parse the parameters
    numberOfReducers = Integer.parseInt(args[0]);
    inputPath = new Path(args[1]);
    outputDir = new Path(args[2]);
    filterPrefix = new String(args[3]);
	
    // Configuarion object
    Configuration conf = this.getConf();
    conf.set("prefix", filterPrefix);

    // Define a new job with the above configuration
    Job job = Job.getInstance(conf); 

    // Assign a name to the job
    job.setJobName("Lab 2");
    
    // Set path of the input file/folder (if it is a folder, the job reads all the files in the specified folder) for this job
    FileInputFormat.addInputPath(job, inputPath);
    
    // Set path of the output folder for this job
    FileOutputFormat.setOutputPath(job, outputDir);
    
    // Specify the class of the Driver for this job
    job.setJarByClass(DriverBigData.class);
    
    // Set job input format
    job.setInputFormatClass(TextInputFormat.class);

    // Set job output format
    job.setOutputFormatClass(TextOutputFormat.class);
     
    // Set map class
    job.setMapperClass(MapperBigData.class);
    
    // Set map output key and value classes
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    
    // Set reduce class
    job.setReducerClass(ReducerBigData.class);
     
    // Set reduce output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    // Set number of reducers
    job.setNumReduceTasks(numberOfReducers);
   
   
    // Execute the job and wait for completion
    if (job.waitForCompletion(true)==true)
    	exitCode=0;
    else
    	exitCode=1;
   
    long selectedWordsCount = job.getCounters().findCounter("COUNTERS", "SELECTED_WORDS").getValue();
    long unselectedWordsCount = job.getCounters().findCounter("COUNTERS", "UNSELECTED_WORDS").getValue();

    System.out.println("\n\n" + "Selected words: " + selectedWordsCount + "; Not selected words: " + unselectedWordsCount);

    return exitCode;
   
  }
  

  /* Main of the driver */
  
  public static void main(String args[]) throws Exception {
	  // Exploit the ToolRunner class to "configure" and run the Hadoop application
    int res = ToolRunner.run(new Configuration(), new DriverBigData(), args);

    System.exit(res);
  }
}
