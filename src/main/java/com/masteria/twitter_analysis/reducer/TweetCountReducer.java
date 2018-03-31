package com.masteria.twitter_analysis.reducer;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TweetCountReducer extends Reducer<TweetKey, MappedTweet, Text, Text> {

    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    public void reduce(TweetKey inputKey, Iterable<MappedTweet> inputValues, Context context)
            throws IOException, InterruptedException {

        ArrayList<MappedTweet> tweets = this.toList(inputValues);
        List<MappedTweet> lastTweets = this.getLastTweets(tweets);

        int tweetsPerUser = this.getTweetsPerUser(tweets);
        double volumeMomentum = this.getVolumeMomentum(lastTweets);
        double popularityMomentum = this.getPopularityMomentum(lastTweets);

        if (inputKey.getUser() == null || inputKey.getUser().compareTo("") == 0) {
            System.out.println("EMPTY");
        }

        this.outputKey.set(inputKey.getUser());
        this.outputValue.set(tweetsPerUser + "," + volumeMomentum + "," + popularityMomentum);

        context.write(this.outputKey, this.outputValue);
    }

    private int getTweetsPerUser(List<MappedTweet> values) {
        int total = 0;

        for (MappedTweet value : values) {
            total += value.getCount().get();
        }

        return total;
    }

    private ArrayList<MappedTweet> toList(Iterable<MappedTweet> inputValues) {
        ArrayList<MappedTweet> tweets = new ArrayList<MappedTweet>();

        for (MappedTweet value : inputValues) {
            tweets.add(new MappedTweet(value.getCount().get(), value.getReTweets().get(), value.getTimestamp().get()));
        }

        return tweets;
    }

    private List<MappedTweet> getLastTweets(List<MappedTweet> tweets) {
        return tweets.subList(Math.max(tweets.size() - 20, 0), tweets.size());
    }

    private double getVolumeMomentum(List<MappedTweet> lastTweets) {
        if (lastTweets.size() < 2) {
            return 0;
        }

        long fullDifference = this.getDiff(lastTweets.get(0), this.getLastElement(lastTweets));
        long halfDifference = this.getDiff(this.getMiddleElement(lastTweets), this.getLastElement(lastTweets));

        double volumeMomentum = (double) (fullDifference - halfDifference) / fullDifference;

        if (Double.isNaN(volumeMomentum)) {
            return 0;
        }

        return volumeMomentum;
    }

    private MappedTweet getMiddleElement(List<MappedTweet> tweets) {
        return tweets.get(this.getMiddlePosition(tweets));
    }

    private int getMiddlePosition(List<MappedTweet> tweets) {
        return tweets.size() / 2;
    }

    private MappedTweet getLastElement(List<MappedTweet> tweets) {
        return tweets.get(tweets.size() - 1);
    }

    private long getDiff(MappedTweet first, MappedTweet last) {
        return last.getTimestamp().get() - first.getTimestamp().get();
    }

    private double getPopularityMomentum(List<MappedTweet> lastTweets) {
        if (lastTweets.size() < 2) {
            return 0;
        }

        long halfCountRT = this.getReTweetsInInterval(lastTweets, this.getMiddlePosition(lastTweets), lastTweets.size());
        long fullCountRT = this.getReTweetsInInterval(lastTweets, 0, lastTweets.size());

        double popularityMomentum = (double) halfCountRT / fullCountRT;

        if (Double.isNaN(popularityMomentum)) {
            return 0;
        }

        return popularityMomentum;
    }

    private long getReTweetsInInterval(List<MappedTweet> tweets, int start, int end) {
        long reTweets = 0;

        for (int i = start; i < end; i++) {
            reTweets += tweets.get(i).getReTweets().get();
        }

        return reTweets;
    }
}
