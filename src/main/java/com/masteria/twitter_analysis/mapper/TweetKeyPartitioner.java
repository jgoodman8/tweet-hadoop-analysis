package com.masteria.twitter_analysis.mapper;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.mapreduce.Partitioner;

public class TweetKeyPartitioner extends Partitioner<TweetKey, MappedTweet> {

    /**
     * Sets the username hash module as partition key.
     * Due to key object contains both username and timestamp, a default hash partitioner cannot be used.
     *
     * @param tweetKey
     * @param tweetValue
     * @param numPartitions
     * @return
     */
    @Override
    public int getPartition(TweetKey tweetKey, MappedTweet tweetValue, int numPartitions) {
        return Math.abs(tweetKey.getUser().hashCode()) % numPartitions;
    }

}
