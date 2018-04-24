package com.masteria.twitter_analysis;

import com.masteria.twitter_analysis.io.TweetInputFormat;
import com.masteria.twitter_analysis.io.TweetOutputFormat;
import com.masteria.twitter_analysis.mapper.TweetCountMapper;
import com.masteria.twitter_analysis.mapper.TweetKeyPartitioner;
import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.TweetKey;
import com.masteria.twitter_analysis.model.UserStats;
import com.masteria.twitter_analysis.reducer.TweetCountReducer;
import com.masteria.twitter_analysis.reducer.TweetKeyGroupComparator;
import com.masteria.twitter_analysis.reducer.TweetKeySortComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class TwitterAnalysisJob extends Configured implements Tool {

    /**
     * Sets up a Hadoop job with custom reader, writer. Also custom grouping, partitioner and key comparator are set
     *
     * @param args Contains the input and output file routes
     * @return It will return 1 when the execution is completed
     * @throws IOException
     * @throws ClassNotFoundException
     * @throws InterruptedException
     */
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(getConf());
        job.setJarByClass(TwitterAnalysisJob.class);
        job.setJobName("TweetsAnalysis");

        job.setInputFormatClass(TweetInputFormat.class);
        job.setOutputFormatClass(TweetOutputFormat.class);

        job.setPartitionerClass(TweetKeyPartitioner.class);

        job.setMapperClass(TweetCountMapper.class);
        job.setReducerClass(TweetCountReducer.class);

        job.setMapOutputKeyClass(TweetKey.class);
        job.setMapOutputValueClass(MappedTweet.class);

        job.setGroupingComparatorClass(TweetKeyGroupComparator.class);
        job.setSortComparatorClass(TweetKeySortComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(UserStats.class);

        job.setNumReduceTasks(2);

        this.setFileDirectories(job, args);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * Sets the input/output paths to the given Hadoop job
     *
     * @param job  Hadoop job to configure
     * @param args Contains the input and output file routes
     * @throws IOException
     */
    private void setFileDirectories(Job job, String[] args) throws IOException {
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        outputPath.getFileSystem(getConf()).delete(outputPath, true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }
}
