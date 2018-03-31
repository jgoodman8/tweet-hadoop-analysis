package com.masteria.twitter_analysis.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.masteria.twitter_analysis.model.Tweet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class TweetReader extends RecordReader<LongWritable, Tweet> {

    private long splitStart, splitEnd, currentPosition;
    private FSDataInputStream fileSystemInput;
    private LineReader lineReader;
    private ObjectMapper mapper = new ObjectMapper();

    private Text line = new Text();
    private LongWritable key = new LongWritable();
    private Tweet value;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        FileSplit fileSplit = (FileSplit) inputSplit;
        Configuration configuration = context.getConfiguration();
        Path path = fileSplit.getPath();
        FileSystem fs = path.getFileSystem(configuration);

        this.fileSystemInput = fs.open(path);

        this.splitStart = fileSplit.getStart();
        this.splitEnd = this.splitStart + fileSplit.getLength();

        this.fileSystemInput.seek(this.splitStart);

        this.lineReader = new LineReader(this.fileSystemInput);

        if (this.splitStart != 0) {
            Text dummyValue = new Text();
            this.splitStart += this.lineReader.readLine(dummyValue, 0,
                    (int) Math.min(Integer.MAX_VALUE, this.splitEnd - this.splitStart));
        }

        this.currentPosition = this.splitStart;
    }


    @Override
    public boolean nextKeyValue() throws IOException {
        this.key.set(this.currentPosition);

        if (this.currentPosition > this.splitEnd) {
            this.key = null;
            this.value = null;
            return false;
        }

        int lineLength = this.lineReader.readLine(this.line);
        if (lineLength == 0) {
            this.key = null;
            this.value = null;
            return false;
        }

        this.currentPosition += lineLength;
        this.value = mapper.readValue(this.line.toString(), Tweet.class);

        return true;
    }

    @Override
    public LongWritable getCurrentKey() {
        return this.key;
    }

    @Override
    public Tweet getCurrentValue() {
        return this.value;
    }

    @Override
    public float getProgress() {
        if (this.splitStart == this.splitEnd) {
            return 0.0f;
        }

        return Math.min(1.0f, (this.currentPosition - this.splitStart) / (float) (this.splitEnd - this.splitStart));
    }

    @Override
    public void close() throws IOException {
        if (this.lineReader != null) {
            this.fileSystemInput.close();
        }
    }
}
