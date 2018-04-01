package com.masteria.twitter_analysis.model;

import java.util.List;

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

    public int size() {
        return this.tweets.size();
    }

    public int getMiddlePosition() {
        return this.tweets.size() / 2;
    }

    public long getReTweetsInInterval(int from, int to) {
        long reTweets = 0;

        for (int i = from; i < to; i++) {
            reTweets += this.tweets.get(i).getReTweets().get();
        }

        return reTweets;
    }

    public long getTimestampDiffFrom(int from) {
        int lastPosition = this.tweets.size() - 1;

        return this.tweets.get(lastPosition).getTimestamp().get() - this.tweets.get(from).getTimestamp().get();
    }
}
