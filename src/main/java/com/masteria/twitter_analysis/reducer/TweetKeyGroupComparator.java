package com.masteria.twitter_analysis.reducer;

import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TweetKeyGroupComparator extends WritableComparator {

    public TweetKeyGroupComparator() {
        super(TweetKey.class, true);
    }

    /**
     * Due to key object contains both username and timestamp, a default key comparator cannot be used. This method
     * provides a comparision by username.
     *
     * @param a
     * @param b
     * @return
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TweetKey keyA = (TweetKey) a;
        TweetKey keyB = (TweetKey) b;

        return keyA.getUser().compareTo(keyB.getUser());
    }
}
