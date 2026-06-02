package it.polito.bigdata;

import java.io.IOException;
import java.util.HashMap;

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
                    Text> {// Output value type

    HashMap<String, Integer> countryCounts;


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

    		String[] lineEntries = value.toString().split(",");
    		String country = lineEntries[3];
    		String plan = lineEntries[4];

    		context.write(new Text(country), new Text(plan))
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {

                return;
    }
}
