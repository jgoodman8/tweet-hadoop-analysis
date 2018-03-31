package com.masteria.twitter_analysis.reducer;

import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class _TweetKeyComparator extends WritableComparator {

    public _TweetKeyComparator() {
        super(TweetKey.class, true);
    }

    @Override
    @SuppressWarnings("rawtypes")
    public int compare(WritableComparable a, WritableComparable b) {

        MappedTweet valueA = (MappedTweet) a;
        MappedTweet valueB = (MappedTweet) b;

        return (int) valueA.getTimestamp().get() - (int) valueB.getTimestamp().get();
    }
}
