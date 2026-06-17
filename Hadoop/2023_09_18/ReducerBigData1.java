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
                Text,           // Input value type
                Text,           // Output key type
                Text> {         // Output value type

    String top_1_SID;
    int top_1_lifespan;

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {

        top_1_SID = "";
        top_1_lifespan = 0;

        return;
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

        // Receives: SID, [OnAir1, OnAir2, OnAir3, ...]

        // Comptue lifespan
        String first = "";
        String latest = "";
        for (Text value:values) {
            String on_air_date = value.toString();
            if (first.equals("")) {
                first = on_air_date;
            }
            if (latest.equals("")) {
                latest = on_air_date;
            }

            int diffWithFirst = Diff.diffDatesInDays(on_air_date, first);
            int diffWithLatest = Diff.diffDatesInDays(latest, on_air_date);
            if (diffWithFirst > 0) {
                first = on_air_date;
            } else if (diffWithLatest > 0) {
                latest = on_air_date;
            }
        }

        String SID = key.toString();
        int lifespan = Diff.diffDatesInDays(first, latest);

        if (top_1_SID == "" && top_1_lifespan==0) {
            top_1_SID = SID;
            top_1_lifespan = lifespan;
            return;
        }

        if (top_1_lifespan == lifespan) {
            if (SID.compareTo(top_1_SID))) < 0) {
                top_1_lifespan = lifespan;
                top_1_SID = SID;
            }
        } else if (top_1_lifespan < lifespan) {
            top_1_lifespan = lifespan;
            top_1_SID = SID;
        }

		}

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        // This may write ""\t0 to the file if the reducer receives no items,
        // however we are sure that in the second job this entry will be pruned
        context.write(new Text(top_1_SID), new Text(top_1_lifespan));
        return;
    }
}
