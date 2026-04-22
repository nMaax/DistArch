package it.polito.bigdata.hadoop.lab;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Lab - Reducer
 */

/* Set the proper data types for the (key,value) pairs */
class ReducerBigData1 extends Reducer<
                Text,           // Input key type
                Text,           // Input value type
                Text,           // Output key type
                FloatWritable> {  // Output value type
    
    @Override
    protected void reduce(
        Text key, // Input key type
        Iterable<Text> values, // Input value type
        Context context) throws IOException, InterruptedException {

		/* Implement the reduce method */
        
        /* 
            Single pass stats can be done using Moments, as in the following:
            NOT ADVICED due to numerical instability

            def single_pass_stats(data):
                n = 0
                mu = 0.0
                M2 = 0.0

                for x in data:
                    n += 1
                    delta = x - mu
                    mu += delta / n # Note: here we are not doing the avg of avgs; we are weighting using that / n
                    delta2 = x - mu
                    M2 += delta * delta2

                if n < 2:
                    return mu, 0.0

                variance = M2 / (n - 1)  # Sample variance
                std_dev = math.sqrt(variance)
                
                return mu, std_dev
            
            Otherwise one simply multiple fors, one after the other, O-big complexit is the same
        */

        // At this point we are given (userID, <iterable of all products user reviewed and their score, as strings>)
        
        // I cannot iterate on the iterable multiple times, and I cannot infer the number of products reviewed by the user beforehand,
        // so I need to store products/scores locally with two options: a very long array, or some LinkedList --- given also the assumption that "you can assume that the number of ratings per user is small";
        
        // I choosed arrays
        int M = 1000; // 1000 will be sufficient, I suppose, knowing much of it will be left empty --- but hoepfully java will optimize it under the hood

        // Prepare two arrays to store products and score
        // *** NOTE ***: we do local calculations in double, but then write float on network for efficiency
        String[] products = new String[M];
        double[] scores = new double[M];

        // Track products and scores while consuming the iterable
        int idx = 0;
        for (Text productScore : values) {
            // Convert and split the Text
            String[] productScoreSplit = productScore.toString().split(",");

            // Extract product and score
            String product = productScoreSplit[0];
            double score = Double.parseDouble(productScoreSplit[1]);

            // Store products and stores in the arrays to iterate on them later
            products[idx] = product;
            scores[idx] = score;

            idx++;
        }
        int count = idx; // Logically, the last idx is equivalent to the count

        if (count == 0){
            return; // Do not even emit anything, just skip
        } else if (count == 1) {
            context.write(
                new Text(products[0]), // key: product 
                new FloatWritable((float) 0.0) // value: normalized score
            );
            return; // Need to return immediately, otherwise the rest of the code would be run and we would have duplicates
        }

        // Now the above arrays will be of lenght M >> count, but most of their entries will be empty (until index=count)
        // We will movce them to resized arrays, and let the garbage collector clean up memory automatically
        products = Arrays.copyOfRange(products, 0, count); // Alternativelly: products = Arrays.copyOf(products, count);
        scores = Arrays.copyOfRange(scores, 0, count);

        // *** Extract mu ***
        // Just for optimization, you could track mu directly inside the above for instead of dedicating a different one
        // Anyway O-complexity is still the same, so I will keep it this way just to be clean
        double sum = 0;
        for (int i=0; i<count; i++) {
            double score = scores[i];
            sum = sum + score;
        }
        double mu = sum / count;
        
        // *** Extract std ***
        // Also std could have been done in one for, mergin everything togheter like proposed with mu
        // Anyway it becomes more error prone, so still I keep a separate for for it, as O-complexity is still the same
        double sumOfSquaredDiffs = 0;
        for (int i=0; i<count; i++) {
            double score = scores[i];
            double diff = score - mu;
            sumOfSquaredDiffs = sumOfSquaredDiffs + Math.pow(diff, 2);
        }
        double std = 0;
        if (count>1) { // If the user has only one review there is no std, and count-1 returns a NaN (NOTE: THIS IS USELESS FOR THE GUARD WE DID ABOVE, BUT I KEEP IT FOR COMPLETENESS)
            std = Math.sqrt(sumOfSquaredDiffs / (count-1));
        }
        
        // *** Apply normalization and write it down ***
        for(int i=0; i<count; i++) {
            // Fetch product and compute normalized score
            String product = products[i];

            // Even if we checked that count > 2, we could still encounter edge cases where the user gave the same score to everyone, or std below a given threshold
            // Such cases could still lead to NaNs due to 0/0 division during normalization, in such cases I simply write 0.0 and call it a day
            double score = scores[i];
            double normalizedScore = 0;
            if (std > 1e-8) {
                normalizedScore = (score - mu) / std; 
            }

            // Write
            context.write(
                new Text(product), // key: product 
                new FloatWritable((float) normalizedScore) // value: normalized score
            );
        }
    }
}
