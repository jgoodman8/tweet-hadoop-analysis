package com.masteria.twitter_analysis.mapper;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.Tweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TweetCountMapper extends Mapper<LongWritable, Tweet, TweetKey, MappedTweet> {

    private static final int VALUE_COUNT = 1;
    private MappedTweet outputValue = new MappedTweet();
    private SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

    @Override
    public void map(LongWritable inputKey, Tweet inputValue, Context context) throws IOException, InterruptedException {

        long timestamp = this.toTimestamp(inputValue.getTimestamp());

        TweetKey outputKey = new TweetKey(inputValue.getUser(), timestamp);

        this.outputValue.setCount(VALUE_COUNT);
        this.outputValue.setTimestamp(timestamp);
        this.outputValue.setReTweets(inputValue.getReTweets());

        context.write(outputKey, this.outputValue);
    }

    private long toTimestamp(String date) {
        try {
            Date parseDate = this.dateFormat.parse(date);
            return parseDate.getTime();
        } catch (ParseException exception) {
            System.out.println(exception.getMessage());
            return 0;
        }
    }
}
