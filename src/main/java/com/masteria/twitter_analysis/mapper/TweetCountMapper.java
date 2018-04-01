package com.masteria.twitter_analysis.mapper;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.Tweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TweetCountMapper extends Mapper<LongWritable, Tweet, TweetKey, MappedTweet> {

    private static final int VALUE_COUNT = 1;
    private TweetKey outputKey = new TweetKey();
    private MappedTweet outputValue = new MappedTweet();

    @Override
    public void map(LongWritable inputKey, Tweet inputValue, Context context) throws IOException, InterruptedException {

        long timestamp = inputValue.getTimestamp();

        this.outputKey.setUser(inputValue.getUser());
        this.outputKey.setTimestamp(timestamp);

        this.outputValue.setCount(VALUE_COUNT);
        this.outputValue.setTimestamp(timestamp);
        this.outputValue.setReTweets(inputValue.getReTweets());

        context.write(this.outputKey, this.outputValue);
    }
}
