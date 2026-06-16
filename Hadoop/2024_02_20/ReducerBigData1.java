package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Exam - Reducer 1
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                NullWritable,    // Input value type
                Text,           // Output key type
                Text> {  // Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<NullWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

		// Will receive "UserID,ItemID": [Null, Null, ...]

		// DISTINCT PATTERN:
		// Decouple User and Item, now that we merged them via the mapper
		// they are unique and distinct pairs

		String[] userItem = key.toString().split(",");
		String user = userItem[0];
		String item = userItem[1];

        context.write(new Text(user), new Text(item)); // This writes with a `\t` (tab) separating items
		}

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
