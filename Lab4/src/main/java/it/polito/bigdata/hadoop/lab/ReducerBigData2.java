package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                Text,           // Input key type
                FloatWritable,    // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<FloatWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

        // This will receive all the normalized scores associated to the same product
        // Simply do a avg out of them, and write them down

        // Track sum and count to compute average
        // Again, calculations in double, but then I convert to float
        double sum = 0;
        int count = 0;
        for(FloatWritable value:values) {
            float normalizedScore = value.get(); // Converts FloatWritable into float
            sum = sum + normalizedScore;
            count++; 
        }
        
        // Compute average of the normalized scores
        double avgNormalizedScore = (sum / count);

        // key can be passed as it is, no need to convert or re-elaborate it, it is simply the product
        context.write(key, new FloatWritable((float) avgNormalizedScore));
    }
}
