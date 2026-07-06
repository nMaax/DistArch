package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exam - Reducer 1
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                IntWritable,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<IntWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

        // Receives IID, [2020, 2019, 2019, 2020, 2019, 2020, ...]
        // as an iterable of values

        // I will count the purchases for each year, distinguishing
        // between 2019 and 2020
        String IID  = key.toString();
        int count_19 = 0;
        int count_20 = 0;
        for (IntWritable value:values) {
            int year = value.get();
            if (year == 2019) {
                count_19++;
            } else if (year == 2020) {
                count_20++;
            }
        }

        // No purchases in 2019
        if (count_19 == 0) {
            return;
        }

        // Only consider when count_20 > count_19
        if (count_19 >= count_20) {
            return;
        }

        // Compute ratio
        double ratio = (count_20 - count_19) / count_19;

        // To write in the given format, I just dump all in a text with a custom string
        // and put null in the value
        context.write(new Text(IID  + "," + ratio), NullWritable.get());
    	
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
