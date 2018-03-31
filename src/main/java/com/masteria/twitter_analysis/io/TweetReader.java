package com.masteria.twitter_analysis.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.masteria.twitter_analysis.model.Tweet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class TweetReader { //  extends RecordReader<Text, Tweet>

//    private long splitStart, splitPosition, splitLength;
//    private BufferedReader inputBuffer;
//    private FSDataInputStream fileSystemInput;
//    private ObjectMapper mapper = new ObjectMapper();
//    private Text key = new Text();
//    private Tweet value ;

//    @Override
//    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
//        FileSplit fileSplit = (FileSplit) inputSplit;
//        Configuration configuration = context.getConfiguration();
//        Path path = fileSplit.getPath();
//        FileSystem fs = path.getFileSystem(configuration);
//
//        this.fileSystemInput = fs.open(path);
//        this.inputBuffer = new BufferedReader(new InputStreamReader(this.fileSystemInput));
//
//        this.splitStart = fileSplit.getStart();
//        this.splitLength = (this.splitStart + fileSplit.getLength()) - this.splitStart;
//    }
//
//    @Override
//    public boolean nextKeyValue() throws IOException {
//        String line = this.inputBuffer.readLine();
//
//        if (line != null) {
//            Tweet mappedTweet = mapper.readValue(line, Tweet.class);
//            this.key = new Text(mappedTweet.getId());
//            this.value = mappedTweet;
//
////            System.out.println(mappedTweet.toString());
//
//            this.splitPosition += line.length();
//
//            return true;
//        }
//
//        return false;
//    }
//
//    @Override
//    public Text getCurrentKey() {
//        return this.key;
//    }
//
//    @Override
//    public Tweet getCurrentValue() {
//        return this.value;
//    }
//
//    @Override
//    public float getProgress() {
//        return Math.min(1.0f, (this.splitPosition - this.splitStart) / this.getSplitLength());
//    }
//
//    @Override
//    public void close() throws IOException {
//        this.fileSystemInput.close();
//    }
//
//    private float getSplitLength() {
//        return (float) this.splitLength;
//    }
}
