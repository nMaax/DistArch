package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Exam - Reducer 1
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,               // Input key type
                BooleanWritable,    // Input value type
                Text,               // Output key type
                NullWritable> {     // Output value type

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

        // Will receive: (CID, [1, 1, 0, 1, 0, ...])

        String CID = key.toString();
        count_recorded = 0;
        count_tot = 0;
        for (IntWritable value:values) {
            count_recorded += value.get();
            count_tot++;
        }


        if (count_recorded == count_tot) {
            String message = "All recorded";
            context.write(new Text(CID + "," + message), NullWritable.get());
        } else if (count_recorded == 0) {
            String message = "No recorded lectures";
            context.write(new Text(CID + "," + message), NullWritable.get());
        } // Assuming the standard key, value context.write would default to \t divisor

		}

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
