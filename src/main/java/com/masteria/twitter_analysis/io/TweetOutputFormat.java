package com.masteria.twitter_analysis.io;

import com.masteria.twitter_analysis.model.UserStats;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class TweetOutputFormat extends TextOutputFormat<Text, UserStats> {

    private static final String DEFAULT_FILE_NAME = "stats";

    public RecordWriter<Text, UserStats> getRecordWriter(TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException {

        Path outputDir = FileOutputFormat.getOutputPath(taskAttemptContext);
        Path outputFilePath = new Path(outputDir.getName() + "/" + DEFAULT_FILE_NAME + "_" +
                taskAttemptContext.getTaskAttemptID().getTaskID());

        FileSystem fileSystem = outputFilePath.getFileSystem(taskAttemptContext.getConfiguration());
        FSDataOutputStream outputStream = fileSystem.create(outputFilePath, false);

        return new TweetWriter(outputStream);
    }

}


