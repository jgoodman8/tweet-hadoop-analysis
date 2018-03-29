package com.masteria.twitter_analysis.reducer;

import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class TweetCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int totalTweetsPerUser = this.getTotalItems(values);
        IntWritable outputValue = new IntWritable(totalTweetsPerUser);

        context.write(key, outputValue);
    }

    private int getTotalItems(Iterable<IntWritable> values) {
        int total = 0;

        for(IntWritable value : values) {
            total += value.get();
        }

        return total;
    }
}
