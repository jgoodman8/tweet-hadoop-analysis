package com.masteria.twitter_analysis.io;

import com.masteria.twitter_analysis.model.Tweet;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class TweetInputFormat extends FileInputFormat<LongWritable, Tweet> {

    @Override
    public RecordReader<LongWritable, Tweet> createRecordReader(InputSplit inputSplit, TaskAttemptContext context)
            throws IOException, InterruptedException {
        return new TweetReader();
    }

}
