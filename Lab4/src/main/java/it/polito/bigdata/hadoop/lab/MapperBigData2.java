package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    FloatWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */

            // Now we simply want to redistribute all normalized scores of a given product to the same reducer
            // So we simply use the product as key, and the normalized scores as values, then the reducer will compute the average out of it 

            // Fetch the line and prepare data
            String line = value.toString();
            String[] splittedLine = line.split("\t");
            String product = splittedLine[0];
            float normalizedScore = Float.parseFloat(splittedLine[1]);

            // Emit
            context.write(
                new Text(product),
                new FloatWritable(normalizedScore)
            );
    }
}
