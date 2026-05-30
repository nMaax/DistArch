package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Basic MapReduce Project - Reducer1
 */
class ReducerBigData extends Reducer<Text, // Input key type
    Text, // Input value type
    Text, // Output key type
    IntWritable> { // Output value type

  @Override
  protected void reduce(
      Text key, // Input key type
      Iterable<Text> values, // Input value type
      Context context) throws IOException, InterruptedException {

    // Hadoop will automatically serve us countries in order, where each
    // key (country) will be associated to an iterable of values "Continent,1"
    // to it associated

    Text country = key; // Just for clarity

    // Every country is associated with at most 1 continent, so I can decalre it
    // outside the loop
    String continent = "";

    // I dont remember if iterables have "lenght" property,
    // I could either:
    // - just iterate as value:values
    // - convert the iterable in an array and then call
    // for(...; i<values.length;...)

    int total = 0; // We want to cumulate the "1"s
    for (Text value : values) {
      String valueString = value.toString();

      // We suppose to receive value as: "continent,1"
      String[] continentAndCount = valueString.split(",");

      // Only fetch the continent if it is the first count
      // (small optimization)
      if (total == 0) {
        continent = continentAndCount[0];
      }

      // Parse the count as integer
      int count = Integer.parseInt(continentAndCount[1]);

      // Add up the count to the total
      total += count;
    }

    // Only if the minimum number of students is met, we emit
    // thus we will emit (continent, 1) for every country in such continent that
    // has at least 1000 students
    if (total >= 1000) {
      // Remind that Hadoop wil write this in a file in the format `key\tvalue`
      context.write(new Text(continent), new IntWritable(1));
    }
  }
}
