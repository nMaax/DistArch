package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Exam - Reducer 1
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                IntWritable> {  // Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

		String brand = key.toString();
		boolean hasMusic = false;
		boolean hasMotor = false;
		for (Text value:values) {
            category = value.toString()
            if (category.equals("Musical Instruments")){
                hasMusic = true;
            } elif (category.equals("Motorcycles")) {
                hasMotor = true:
            }
		}
		if (hasMusic && hasMotor) {
            context.write(key, NullWritable.get())
		}
        
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
