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
                NullWritable> {  // Output value type

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

		// This will receive: Italy, [free, free, free, academic, free, academic, ...]

		int countFree = 0;
		int countAcademic = 0;
		int countTotal = 0;

		for (Text value:values) {
            plan = value.toString();
            if (plan.equals("free")) {
                countFree++;
            } else if (plan.equals("acedemic")) {
                countAcademic++;
            }

            countTotal++;
		}

		float percFree = (float) countFree / countTotal;
		float percAcademic = (float) countAcademic / countTotal;

		if (percFree >= 0.3 && percAcademic >= 0.3){
            context.write(key, NullWritable.get())
		}
        
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
