package it.polito.bigdata.hadoop;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Basic MapReduce Project - Mapper
 */
class MapperBigData extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    IntWritable> {// Output value type
    
    private int n;

    @Override
    protected void setup(Context context) {
        // Retrieve n from configuration, default to 2 if not found
        Configuration conf = context.getConfiguration();
        this.n = conf.getInt("ngram.size", 2);
    }

    @Override
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context
    ) throws IOException, InterruptedException {

            int n = this.n; // quick fix
            String[] words = value.toString().split("\\s+");

            // Iterate over the set of words
            for(int i=0; i<=words.length-n; i++) {

              System.out.println("Processing line: " + value.toString());
              System.out.println("Current n: " + n);
              System.out.println("Current words: " + Arrays.toString(words));
              
              String[] slicedArray = Arrays.copyOfRange(words, i, i+n);

              System.out.println("Current slice: " + Arrays.toString(slicedArray));
              
              String[] ngram = new String[n];
              for (int j=0;j<n;j++) {
                String word = slicedArray[j];
                String cleanedWord = cleanWord(word);
                if(cleanedWord.isEmpty()) {
                  continue; // Skip empty words
                }
                ngram[j] = cleanedWord;
              }

              String ngramString = String.join(" ", ngram);

              // emit the pair (ngram, 1)
              context.write(
                  new Text(ngramString),
                  new IntWritable(1)
              );
            }
    }

    protected String cleanWord(String word) {
        String cleanedWord = word.toLowerCase().replaceAll("[^a-zA-Z]", "");
        return cleanedWord;
    }
}
