package com.masteria.twitter_analysis.io;

import com.masteria.twitter_analysis.model.UserStats;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class TweetWriter extends RecordWriter<Text, UserStats> {

    private DataOutputStream outputStream;

    private static final String CSV_SEPARATOR = ",";
    private static final String UTF_8 = "UTF-8";
    private static final byte[] NEW_LINE;
    static {
        try {
            NEW_LINE = "\n".getBytes(UTF_8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + UTF_8 + " encoding");
        }
    }

    public TweetWriter(DataOutputStream dataOutputStream) {
        this.outputStream = dataOutputStream;
    }

    @Override
    public void write(Text key, UserStats value) throws IOException, InterruptedException {
        if (key == null) {
            return;
        }

        String outputRegistry = this.makeOutputRegistry(key, value);
        this.outputStream.write(outputRegistry.getBytes(UTF_8));

        outputStream.write(NEW_LINE);
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        outputStream.close();
    }

    private String makeOutputRegistry(Text key, UserStats value) {
        return key.toString() + CSV_SEPARATOR + value.getTweetsCount().get() + CSV_SEPARATOR +
                value.getVolumeMomentum() + CSV_SEPARATOR + value.getPopularityMomentum();
    }
}
