package com.masteria.twitter_analysis.model;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TweetKey implements WritableComparable<TweetKey> {

    private String user;
    private Long timestamp;

    public TweetKey() {
    }

    public TweetKey(String user, Long timestamp) {
        this.user = user;
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public int compareTo(TweetKey tweetKey) {
        return this.user.compareTo(tweetKey.user);
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.user);
        dataOutput.writeLong(this.timestamp);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.user = dataInput.readUTF();
        this.timestamp = dataInput.readLong();
    }

    @Override
    public String toString() {
        return "TweetKey{" +
                "user='" + user + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
