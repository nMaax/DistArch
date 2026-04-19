package it.polito.bigdata.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Lab 2 - Mapper
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type

    String filterPrefix;

    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        filterPrefix = (String) conf.get("prefix");
    }
    
    @Override
    protected void map(
        LongWritable key,   // Input key type
        Text value,         // Input value type
        Context context) throws IOException, InterruptedException {

        String cleaned_values = value.toString().toLowerCase().trim();
        String[] words = cleaned_values.split("\\s+");

        if(cleaned_values.startsWith(filterPrefix)){
            for(String word:words) {
                word = cleanString(word);
                context.write( //emit
                    new Text(word),
                    new IntWritable(1)
                );
            }
            context.getCounter("COUNTERS", "SELECTED_WORDS").increment(words.length);
        } else {
            context.getCounter("COUNTERS", "UNSELECTED_WORDS").increment(words.length);
        }
    }

    private String cleanString(String s){
        String cleanedWord = s.toLowerCase().trim().replaceAll("[^a-zA-Z]", "");
        return cleanedWord;
    }
}

