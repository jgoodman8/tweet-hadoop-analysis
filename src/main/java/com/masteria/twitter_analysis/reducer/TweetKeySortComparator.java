package com.masteria.twitter_analysis.reducer;

import com.masteria.twitter_analysis.model.TweetKey;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TweetKeySortComparator extends WritableComparator {

    public TweetKeySortComparator() {
        super(TweetKey.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        TweetKey keyA = (TweetKey) a;
        TweetKey keyB = (TweetKey) b;

        return keyA.getUser().compareTo(keyB.getUser()) != 0 ?
                keyA.getUser().compareTo(keyB.getUser()) :
                keyA.getTimestamp().compareTo(keyB.getTimestamp());
    }
}
