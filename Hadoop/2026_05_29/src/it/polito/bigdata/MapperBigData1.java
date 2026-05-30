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
                    IntWritable> {// Output value type

    HashMap<String, Integer> countryCounts;


    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        
                countryCounts = new HashMap<>();
                return;
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

    		/* Implement the map method */
            String[] line = value.toString().split(",");
            String country = line[3];
            String plan = line[4];

            if (plan.equals("free")){
                if (this.countryCounts.get(country) == null) {
                    this.countryCounts.put(country, 1);
                } else {
                    int count = this.countryCounts.get(country);
                    this.countryCounts.put(country, count+1);
                }
            } else {
                // If country is not present this will NOT throw an error
                this.countryCounts.remove(country);
            }

    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
                
                for (String country:this.countryCounts.keySet()) {
                    int count = this.countryCounts.get(country);
                    context.write(new Text(country), new IntWritable(count));
                }

                return;
    }
}
