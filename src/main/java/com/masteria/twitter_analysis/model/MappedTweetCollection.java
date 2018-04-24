package com.masteria.twitter_analysis.model;

import java.util.List;

/**
 * This class encapsulates the methods used over all the tweets with the same key at the Reduce stage.
 */
public class MappedTweetCollection {
    private List<MappedTweet> tweets;

    public MappedTweetCollection(List<MappedTweet> tweets) {
        this.tweets = tweets;
    }

    public MappedTweetCollection getLastTweets() {
        return new MappedTweetCollection(this.tweets.subList(Math.max(this.size() - 20, 0), this.size()));
    }

    public List<MappedTweet> getTweets() {
        return tweets;
    }

    /**
     * Calculates the number of tweets with the same key within a reduce operation
     *
     * @return The collection size
     */
    public int size() {
        return this.tweets.size();
    }

    /**
     * Calculates which is the middle within a grouped-by-key tweet collection
     *
     * @return Middle index of the collection
     */
    public int getMiddlePosition() {
        return this.tweets.size() / 2;
    }

    /**
     * Returns a subset of the collection between the provided indexes
     *
     * @param from First index delimiter
     * @param to   Last index delimiter
     * @return A collection subset
     */
    public long getReTweetsInInterval(int from, int to) {
        long reTweets = 0;

        for (int i = from; i < to; i++) {
            reTweets += this.tweets.get(i).getReTweets().get();
        }

        return reTweets;
    }

    /**
     * Calculates the difference between the last element timestamp and the timestamp of the element at the provided
     * position
     *
     * @param from Collection position
     * @return Timestamp difference
     */
    public long getTimestampDiffFrom(int from) {
        int lastPosition = this.tweets.size() - 1;

        return this.tweets.get(lastPosition).getTimestamp().get() - this.tweets.get(from).getTimestamp().get();
    }
}
