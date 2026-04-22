package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    Text> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */ 

            // The header of the file must be filtered
            String line = value.toString();
            if (key.get() == 0 && line.startsWith("Id,ProductId,UserId,")) {
                return;
            }

            //Prepare data
            String[] lineEntries = line.split(",");
            String productID = lineEntries[1];
            String userID = lineEntries[2];
            String score = lineEntries[6];

            // Simply emit (userID, "product,score")
            context.write(
                new Text(userID), 
                new Text(productID + "," + score)
            );

    }
}
