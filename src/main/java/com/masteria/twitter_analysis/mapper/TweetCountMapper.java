package com.masteria.twitter_analysis.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.Tweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TweetCountMapper extends Mapper<LongWritable, Text, TweetKey, MappedTweet> {

    private static final int VALUE_COUNT = 1;
    private MappedTweet outputValue = new MappedTweet();
    private ObjectMapper mapperJSON = new ObjectMapper();
    private SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

    @Override
    public void map(LongWritable inputKey, Text inputValue, Context context) throws IOException, InterruptedException {
        Tweet tweet = this.mapperJSON.readValue(inputValue.toString(), Tweet.class);

        long timestamp = this.toTimestamp(tweet.getTimestamp());

        TweetKey outputKey = new TweetKey(tweet.getUser(), timestamp);

        this.outputValue.setCount(VALUE_COUNT);
        this.outputValue.setTimestamp(timestamp);
        this.outputValue.setReTweets(tweet.getReTweets());

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
