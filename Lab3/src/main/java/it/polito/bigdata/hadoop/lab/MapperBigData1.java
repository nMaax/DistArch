package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
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
                    IntWritable> {// Output value type
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Prepare data
            String[] lineEntries = value.toString().split(",");
            String userID = lineEntries[0]; // Unused, but still here to be clear
            String[] productIDs = Arrays.copyOfRange(lineEntries, 1, lineEntries.length); // Exclude userID (first entry)
            
            // For product i in products(0, end-1), e.g. from 0 to 9, for 10 products
            for (int i=0; i<productIDs.length-1; i++){
                // For product j in product(i+1, end), e.g. from 1 to 10 for i=1, from 10 to 10 for i=9
                for (int j=i+1; j<productIDs.length; j++){
                    String productA = productIDs[i];
                    String productB = productIDs[j];
                    
                    // Prepare the key (A,B)
                    String[] productPair = new String[2];
                    productPair[0] = productA;
                    productPair[1] = productB;

                    // Sort to ensure consistency (A,B must be the same as B,A)
                    Arrays.sort(productPair);
                    String emitKey = String.join(",", productPair); 

                    // Emit (pair, 1)
                    context.write(
                        new Text(emitKey),
                        new IntWritable(1)
                    );
                }
            }
    }
}
