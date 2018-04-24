package com.masteria.twitter_analysis.reducer;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.MappedTweetCollection;
import com.masteria.twitter_analysis.model.TweetKey;
import com.masteria.twitter_analysis.model.UserStats;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;

public class TweetCountReducer extends Reducer<TweetKey, MappedTweet, Text, UserStats> {

    private Text outputKey = new Text();
    private UserStats outputValue = new UserStats();

    /**
     * On this stage, we receive a collection of MappedTweet objects per each TweetKey. By using the
     * methods defined on this class, the number of tweets per user and the metrics volume momentum and popularity
     * momentum are calculated.
     * <p>
     * This method writes to the context a new output tuple: username as key and a UserStats object as value
     *
     * @param inputKey    A key object as a TweetKey object
     * @param inputValues A iterable collection of MappedTweet objects related to the same username
     * @param context     The Reduce context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void reduce(TweetKey inputKey, Iterable<MappedTweet> inputValues, Context context)
            throws IOException, InterruptedException {

        MappedTweetCollection tweets = this.toMappedTweetCollection(inputValues);
        MappedTweetCollection lastTweets = tweets.getLastTweets();

        int tweetsPerUser = this.getTweetsPerUser(tweets);
        double volumeMomentum = this.getVolumeMomentum(lastTweets);
        double popularityMomentum = this.getPopularityMomentum(lastTweets);

        this.outputKey.set(inputKey.getUser());
        this.outputValue.setTweetsCount(tweetsPerUser);
        this.outputValue.setVolumeMomentum(volumeMomentum);
        this.outputValue.setPopularityMomentum(popularityMomentum);

        context.write(this.outputKey, this.outputValue);
    }

    /**
     * Counts the number of tweets made by user, for the provided collection
     *
     * @param collection
     * @return
     */
    private int getTweetsPerUser(MappedTweetCollection collection) {
        int total = 0;

        for (MappedTweet tweet : collection.getTweets()) {
            total += tweet.getCount().get();
        }

        return total;
    }

    /**
     * For a given collection, calculates the VolumeMomentum metric
     *
     * @param lastTweets
     * @return
     */
    private double getVolumeMomentum(MappedTweetCollection lastTweets) {
        if (lastTweets.size() < 2) {
            return 0;
        }

        long fullDifference = lastTweets.getTimestampDiffFrom(0);
        long halfDifference = lastTweets.getTimestampDiffFrom(lastTweets.getMiddlePosition());

        double volumeMomentum = (double) (fullDifference - halfDifference) / fullDifference;

        if (Double.isNaN(volumeMomentum)) {
            return 0;
        }

        return volumeMomentum;
    }

    /**
     * For a given collection, calculates the PopularityMomentum metric
     *
     * @param lastTweets
     * @return
     */
    private double getPopularityMomentum(MappedTweetCollection lastTweets) {
        if (lastTweets.size() < 2) {
            return 0;
        }

        long halfCountRT = lastTweets.getReTweetsInInterval(lastTweets.getMiddlePosition(), lastTweets.size());
        long fullCountRT = lastTweets.getReTweetsInInterval(0, lastTweets.size());

        double popularityMomentum = (double) halfCountRT / fullCountRT;

        if (Double.isNaN(popularityMomentum)) {
            return 0;
        }

        return popularityMomentum;
    }

    /**
     * Copies and converts the iterable collection of values that the reducer provides, to a collection easily operable.
     * This is also needed due we lose this iterable once it is iterated
     *
     * @param inputValues An iterable collection of MappedTweet objects
     * @return A MappedTweetCollection with the same items as the input iterable
     */
    private MappedTweetCollection toMappedTweetCollection(Iterable<MappedTweet> inputValues) {
        ArrayList<MappedTweet> tweets = new ArrayList<MappedTweet>();

        for (MappedTweet value : inputValues) {
            tweets.add(new MappedTweet(value.getCount().get(), value.getReTweets().get(), value.getTimestamp().get()));
        }

        return new MappedTweetCollection(tweets);
    }
}
