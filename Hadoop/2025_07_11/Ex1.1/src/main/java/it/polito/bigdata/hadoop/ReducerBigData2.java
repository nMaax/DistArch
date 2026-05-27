package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer2
 */
class ReducerBigData extends Reducer<Text, // Input key type
    IntWritable, // Input value type
    Text, // Output key type
    NullWritable> { // Output value type

  @Override
  protected void reduce(
      Text key, // Input key type
      Iterable<IntWritable> values, // Input value type
      Context context) throws IOException, InterruptedException {

    // Hadoop will automatically serve us continents in order, where each
    // key (continent) will be associated to an iterable of values:
    // [1 (int), 1, ...]

    String continent = key.toString(); // Needed later in the output
    int total = 0; // We want to cumulate the "1"s
    for (IntWritable value : values) {
      // Convert to int and cumulate
      int count = value.get();
      total += count;
    }
    // Hadoop wil write in a file in the format `key\tvalue`
    // Since we want the comma separated value we shall rather
    // emit the formated string and a NullWritable
    // remind that NullWritable is a singleton, thus we dont do
    // `new NullWritable`, but `NullWritable.get()`
    context.write(continent + "," + total, NullWritable.get());
  }
}
