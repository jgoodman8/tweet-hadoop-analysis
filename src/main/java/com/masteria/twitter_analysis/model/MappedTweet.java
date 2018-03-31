package com.masteria.twitter_analysis.model;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MappedTweet implements Writable {

    private IntWritable count;
    private LongWritable reTweets, timestamp;

    public MappedTweet() {
        this.count = new IntWritable();
        this.reTweets = new LongWritable();
        this.timestamp = new LongWritable();
    }

    public MappedTweet(int count, long reTweets, long timestamp) {
        this.count = new IntWritable(count);
        this.reTweets = new LongWritable(reTweets);
        this.timestamp = new LongWritable(timestamp);
    }

    public LongWritable getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = new LongWritable(timestamp);
    }

    public LongWritable getReTweets() {
        return reTweets;
    }

    public void setReTweets(long reTweets) {
        this.reTweets = new LongWritable(reTweets);
    }

    public IntWritable getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = new IntWritable(count);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.timestamp.readFields(dataInput);
        this.reTweets.readFields(dataInput);
        this.count.readFields(dataInput);
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.timestamp.write(dataOutput);
        this.reTweets.write(dataOutput);
        this.count.write(dataOutput);
    }

    @Override
    public String toString() {
        return "MappedTweet{" +
                "timestamp=" + timestamp.toString() +
                ", reTweets=" + reTweets.toString() +
                ", count=" + count.toString() +
                '}';
    }
}
