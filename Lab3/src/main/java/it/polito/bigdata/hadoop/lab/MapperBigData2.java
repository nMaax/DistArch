package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab  - Mapper
 */



/* Set the proper data types for the (key,value) pairs */
class MapperBigData2 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    NullWritable,         // Output key type
                    WordCountWritable> {// Output value type
    
    TopKVector<WordCountWritable> topk;

    protected void setup(Context context){
        // Instantiate the TopK array
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("k"));
        this.topk = new TopKVector<WordCountWritable>(k);
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

            // Parse the product pair and its count
            String[] splittedLine = value.toString().split("\t");
            String productPair = splittedLine[0]; 
            int count = Integer.parseInt(splittedLine[1]);

            // Store it in a WordCountWritable object and add it to the topk
            // If count is too small it will discarded automatically by topk
            WordCountWritable productPairCount = new WordCountWritable(productPair, count);
            this.topk.updateWithNewElement(productPairCount);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
        WordCountWritable[] localTopK = (WordCountWritable[]) this.topk.getLocalTopK().toArray(new WordCountWritable[0]);
        
        // Send the local top k with same key (1 reducer only) 
        for(int i=0; i<localTopK.length; i++) { // I do not loop on k as the local top could be shorter than k!
            WordCountWritable productPairCountable = localTopK[i];
            context.write(
                NullWritable.get(),
                productPairCountable
            );
        }
    }
}
