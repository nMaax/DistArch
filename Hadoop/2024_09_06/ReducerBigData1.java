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
                Text,               // Input key type
                FloatWritable,      // Input value type
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
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        /* Implement the reduce method */

        // This will receive (ItemID, [Price1, Price2, ...])

        String itemID = key.toString();

        float min = Float.MAX_VALUE; // Upper bound in java
        float max = 0.0; // Lower bound for our setting (price cant be negative)
        for (FloatWritable value:values){
            float price = value.get();

            // With the first iteration max == min == price
            // Later on max and min will diverge (if more than one price has been set)
            if (price < min) {
                min = price;
            }
            if (price > max) {
                max = price;
            }
        }

        // Merge everything in the key as text for custom formatting (instead of using \t)
        context.write(new Text(itemID + ", " + max + ", " + min), NullWritable.get());

        }
        
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
