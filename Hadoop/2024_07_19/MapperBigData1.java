package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

/**
 * Exam  - Mapper 1
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable,       // Input key type
                    Text,               // Input value type
                    Text,               // Output key type
                    IntWritable> {  // Output value type

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
        String[] lineEntries = line.split(",");

        String CID = lineEntries[1];
        String recorded = lineEntries[6];

        // NOTE: Same as `int recordedFlag = recorded.equals("Yes") ? 1 : 0;`
        int recorded_int = 0;
        if (recorded.equals("Yes") {
            recorded_int = 1;
        }

        context.write(new Text(CID), new IntWritable(recorded_int));

    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
