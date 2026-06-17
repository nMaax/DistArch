package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Exam - Reducer 2
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                Text,                   // Input value type
                Text,                   // Output key type
                Text> {         // Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    @Override
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {
        
		/* Implement the reduce method */

		// NOTE:
		// There will be only one key, as the top-1 pattern must work with only one actual
		// reducer working. Given my design choice of using Null as a key, I could potentially set
		// multiple reducers for the second job, say n, then n-1 would be ignored as all keys would
		// converge to just one reducer.
		// In the MCQ I will then indicate exactly one reducer instance must be setted for the second job
		// for sake of optimization, however also any arbitrary number >= 1 would work

        // Receives Null, [SID1\tLifespan1, SID2\tLifespan2, ...]

		int top_1_lifespan = 0;
		String top_1_SID = "";
		for (Text value:values) {
            String[] SIDAndLifespan = value.toString().split("\t");

            String SID = SIDAndLifespan[0];
            int lifespan = Integer.parseInt(SIDAndLifespan[1]);

            if (top_1_SID == "" && top_1_lifespan==0) {
                top_1_SID = SID;
                top_1_lifespan = lifespan;
                continue;
            }

            if (top_1_lifespan == lifespan) {
                if ((SID.compareTo(top_1_SID)) < 0) {
                    top_1_lifespan = lifespan;
                    top_1_SID = SID;
                }
            } else if (top_1_lifespan < lifespan) {
                top_1_lifespan = lifespan;
                top_1_SID = SID;
            }
    	}

    	context.write(new Text(top_1_SID), new Text(top_1_lifespan));
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
