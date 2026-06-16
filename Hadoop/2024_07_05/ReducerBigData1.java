package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.NullWritable;

/**
 * Exam - Reducer 1
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,    // Input value type
                Text,           // Output key type
                NullWritable> {  // Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */

		// Receives: (SoftwareEngineer, [IT, IT, IT, ES, PO, NE, IT, ES, NE, ...])

        HashMap<String, Integer> countriesCount = new HashMap<>();

        for (Text value:values) {
            String country = value.toString();
            int count = 1;


            // Cant remind the exact API of HashMaps
            if (countriesCount.get(country) != null) {
                count = countriesCount.get(country) + 1;
            }
            countriesCount.put(country, count);
        }

        int numCountriesAdmitted = 0;
        for (int count:countriesCount.values()) {
            if (count >= 30) {
                numCountriesAdmitted++;
            }
        }

        if (numCountriesAdmitted >= 2) {
            context.write(key, NullWritable.get());
        }

		}

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
