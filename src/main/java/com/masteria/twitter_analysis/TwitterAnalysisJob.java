package com.masteria.twitter_analysis;

import com.masteria.twitter_analysis.io.TweetInputFormat;
import com.masteria.twitter_analysis.mapper.TweetCountMapper;
import com.masteria.twitter_analysis.reducer.TweetCountReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class TwitterAnalysisJob extends Configured implements Tool {

    /**
     * TODO: Add JavaDoc
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(TwitterAnalysisJob.class);

        job.setInputFormatClass(TweetInputFormat.class);

        job.setMapperClass(TweetCountMapper.class);
        job.setReducerClass(TweetCountReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        this.setFileDirectories(job, args);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void setFileDirectories(Job job, String[] args) throws IOException {
//        Path inputPath = args[0] == null ? new Path("/root/Documents/cache-1000000-json") : new Path(args[0]);
//        Path outputPath = args[1] == null ? new Path("output_tweets") : new Path(args[1]);
        Path inputPath = new Path("/root/Documents/cache-1000000-json");
        Path outputPath = new Path("output_tweets");
        outputPath.getFileSystem(getConf()).delete(outputPath,true);

        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }
}
