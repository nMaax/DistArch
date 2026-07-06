package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Exam  - Mapper 1
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type

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

            String IID = lineEntries[2];
            String timestamp = lineEntries[3];

            // Cant remind if / should be escaped as //, either way I just want to 
            // get the first 4 characters related to the year
            int year = Integer.parseInt(timestamp.split("/")[0]);

            if (year == 2020 || year == 2019) {
                // emit IID, year
                context.write(new Text(IID), new IntWritable(year));
            }
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
