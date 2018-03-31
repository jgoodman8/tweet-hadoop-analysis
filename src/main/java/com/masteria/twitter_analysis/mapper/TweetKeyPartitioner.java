package com.masteria.twitter_analysis.mapper;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class TweetKeyPartitioner  extends Partitioner<TweetKey, MappedTweet> {

    @Override
    public int getPartition(TweetKey tweetKey, MappedTweet tweetValue, int i) {
        return Math.abs(tweetKey.getUser().hashCode()) % i;
    }

}
