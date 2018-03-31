package com.masteria.twitter_analysis.model;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserStats implements Writable {

    private IntWritable tweetsCount;
    private DoubleWritable volumeMomentum, popularityMomentum;

    public UserStats() {
        this.tweetsCount = new IntWritable();
        this.volumeMomentum = new DoubleWritable();
        this.popularityMomentum = new DoubleWritable();
    }

    public void write(DataOutput dataOutput) throws IOException {
        this.tweetsCount.write(dataOutput);
        this.volumeMomentum.write(dataOutput);
        this.popularityMomentum.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.tweetsCount.readFields(dataInput);
        this.volumeMomentum.readFields(dataInput);
        this.popularityMomentum.readFields(dataInput);
    }

    public IntWritable getTweetsCount() {
        return tweetsCount;
    }

    public void setTweetsCount(int tweetsCount) {
        this.tweetsCount = new IntWritable(tweetsCount);
    }

    public DoubleWritable getVolumeMomentum() {
        return volumeMomentum;
    }

    public void setVolumeMomentum(double volumeMomentum) {
        this.volumeMomentum = new DoubleWritable(volumeMomentum);
    }

    public DoubleWritable getPopularityMomentum() {
        return popularityMomentum;
    }

    public void setPopularityMomentum(double popularityMomentum) {
        this.popularityMomentum = new DoubleWritable(popularityMomentum);
    }

    @Override
    public String toString() {
        return "UserStats{" +
                "tweetsCount=" + tweetsCount.toString() +
                ", volumeMomentum=" + volumeMomentum.toString() +
                ", popularityMomentum=" + popularityMomentum.toString() +
                '}';
    }
}
