package it.polito.bigdata.hadoop.lab;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData2 extends Reducer<
                NullWritable,           // Input key type
                WordCountWritable,    // Input value type
                NullWritable,           // Output key type
                WordCountWritable> {  // Output value type
    
    protected void reduce(
        NullWritable key, // Input key type
        Iterable<WordCountWritable> values, // Input value type
        Context context) throws IOException, InterruptedException {

        // Get k for later use
        Configuration conf = context.getConfiguration();
        int k = Integer.parseInt(conf.get("k"));

        // Fetch all product pairs and their count, and construct a new gloabal top k
        TopKVector<WordCountWritable> globalTopKVector = new TopKVector<WordCountWritable>(k);
        for(WordCountWritable productPairCount:values){
            // Hadoop reuses the same value object while iterating over values
            // Clone it before storing references in TopK
            globalTopKVector.updateWithNewElement(new WordCountWritable(productPairCount));
        }
        WordCountWritable[] globalTopK = (WordCountWritable[]) globalTopKVector.getLocalTopK().toArray(new WordCountWritable[0]);

        // Write the top k pairs
        for(int i=0; i<globalTopK.length; i++){
            WordCountWritable productPairCountable = globalTopK[i];
            context.write(NullWritable.get(), productPairCountable);
        }
    	
    }
}
