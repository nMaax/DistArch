package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper2
 */
class MapperBigData extends Mapper<LongWritable, // Input key type: int, offset of the line
    Text, // Input value type: string, since we read a txt file
    Text, // Output key type: string, identifying the continent
    IntWritable> { // Output value type: IntWritable(1), will always be 1

  protected void map(
      LongWritable key, // Input key type
      Text value, // Input value type
      Context context) throws IOException, InterruptedException {

    // Basic conversion and cleanup of the line
    String line = value.toString().strip();

    // We expect the line to be "continent,1"
    String[] items = line.split("\t"); // Haddop default to \t delimiter when it wrote in Reducer1
    String continent = items[0];
    int count = Integer.parseInt(items[1]); // This will always be 1, here just for clarity

    // I can either parse count to an int (which will always be 1), and re-use it
    // or just ignore it and emit a fresh new IntWritable(1).
    // Either way, the difference is negligeble
    // I do the second and keep the variable above for clarity

    // Emit key: countinent (string), value: 1 (int)
    // Then the reducer simply has to cumulate the 1s
    context.write(new Text(continent), new IntWritable(1));
  }
}
