package com.masteria.twitter_analysis.mapper;

import com.masteria.twitter_analysis.model.Tweet;
import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TweetCountMapper extends Mapper<Text, Tweet, Text, IntWritable> {

    @Override
    protected void map(Text inputKey, Tweet inputValue, Context context) throws IOException, InterruptedException {
        Text outputKey = new Text(inputValue.getUser());
        IntWritable outputValue = new IntWritable(1);

        context.write(outputKey, outputValue);
    }
}
