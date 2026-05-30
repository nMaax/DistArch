package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper1
 */
class MapperBigData extends Mapper<LongWritable, // Input key type: int, offset of the line (we ignore it)
    Text, // Input value type: string, since we read a txt file
    Text, // Output key type: string, identifying the country
    Text> { // Output value type: string, to concatenate country and "1"

  protected void map(
      LongWritable key, // Input key type
      Text value, // Input value type
      Context context) throws IOException, InterruptedException {

    /*
     * The main idea is:
     * - take students.txt as input
     * - map: read file lines and emit ("country"", "continent,1")
     * - reduce: parse and sum on 1s and emit (write on file) "continent" if sum >
     * 1000
     * - map: read file lines and emit ("continent", 1)
     * - reduce: sum on 1s and emit (write on file) "continent,sum"
     */

    // Basic conversion and cleanup of the line
    String line = value.toString().trim();

    // Split the line in an array of strings
    String[] items = line.split(",");

    // Skip header
    if (items[0] == "SID") {
      return;
    }

    // Fetch country
    String country = items[3].trim();
    // Fetch continent
    String continent = items[4].trim();

    // Emit key: country (string), value: tuple of (1 (int), continent (string))
    // Since tuples are not native in Java we can either use String concatenation
    // (then parsing the int on the reducer
    // or some custom class that implements the Writable interface)

    // Another approach would have been to simply emit value: "continent"
    // (without "1"), as it is trivial but I like this one for clarity

    context.write(new Text(country), new Text(continent + "," + 1)); // + act as concatenation between a string and "1"
  }
}
