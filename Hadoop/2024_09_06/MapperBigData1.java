package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exam  - Mapper 1
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable,   // Input key type
                    Text,           // Input value type
                    Text,           // Output key type
                    FloatWritable> {// Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        /* Implement the map method */

        String line = value.toString();
        String lineEntries = line.split(",");

        String itemID = lineEntries[0];
        String startingDate = lineEntries[1];
        String endingDate = lineEntries[2];
        float price = Float.parseFloat(lineEntries[3]);

        String leftBoundDate = "2015/01/01";
        String rightBoundDate = "2023/12/31";

        // NOTE: it is not specified if we should check on ending date too. Thus, I will assume that
        // checking on starting date is enough. In case this wasn't true, a fix is quite trivial. (SEE FIX BELOW)
        // If the price has been set outside date boundaries for 2015-2023, ignore it
        if (startingDate.compareTo(leftBoundDate) < 0 || startingDate.compareTo(rightBoundDate) > 0) {
            return;
        }

        // IN CASE WE WANT TO CHECK ON END DATE TOO
        // The first compareTo() is redundant, but I leave it for clarity.
        if (endingDate.compareTo(leftBoundDate) < 0 || endingDate.compareTo(rightBoundDate) > 0) {
            return;
        }

        context.write(new Text(itemID), new FloatWritable(price));

    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
