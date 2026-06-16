package it.polito.bigdata;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.NullWritable;

/**
 * Exam  - Mapper 1
 */

/* Set the proper data types for the (key,value) pairs */
class MapperBigData1 extends Mapper<
                    LongWritable, // Input key type
                    Text,         // Input value type
                    Text,         // Output key type
                    NullWritable> {// Output value type

    @Override
    protected void setup(Context context)
            throws IOException, InterruptedException {
        return;
    }
    
    protected void map(
            LongWritable key,   // Input key type
            Text value,         // Input value type
            Context context) throws IOException, InterruptedException {

        /* Implement the map method */
        String line = value.toString();
        String[] lineEntries = line.split(",");

        // lineEntries: [SaleTimestamp, UserID, ItemId, SalePrice]
        String timestamp = lineEntries[0];
        String userID = lineEntries[1];
        String itemID = lineEntries[2];

        // If timestamp is in ISO format, or a lexically orderable equivalent (which it is)
        // I can just use the string comparison, otherwise I need a parser
        //
        // I can't remind the exact java syntax at the moment.
        // However, here I mean that if timestamp is
        // before 1/1/2020 OR after 21/12/2023
        // then we return (i.e., skip this line) as we dont need it
        // We suppose to include such dates as those interested in analysis
        String startTime = "2020/01/01-00:00:00"; // We suppose start of day is 0:0:0
        String endTime = "2023/12/21-23:59:59"; // While end is 23:59:59
        if (timestamp.compareTo(startTime) < 0 || timestamp.compareTo(endTime) > 0) {
            return;
        }

        // We emit ("User,Item", Null)
        // the reducer will receive an iterable, not a full structured array
        // thus it will be menageable in memory as long as we dont convert it to an array
        context.write(new Text(userID + "," + itemID), NullWritable.get());
    }

    @Override
    protected void cleanup(Context context)
            throws IOException, InterruptedException {
        return;
    }
}
