package com.masteria.twitter_analysis;

import com.masteria.twitter_analysis.mapper.TweetCountMapper;
import com.masteria.twitter_analysis.mapper.TweetKeyPartitioner;
import com.masteria.twitter_analysis.model.MappedTweet;
import com.masteria.twitter_analysis.model.TweetKey;
import com.masteria.twitter_analysis.reducer.TweetCountReducer;
import com.masteria.twitter_analysis.reducer.TweetKeyGroupComparator;
import com.masteria.twitter_analysis.reducer.TweetKeySortComparator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class TwitterAnalysisJob extends Configured implements Tool {

    /**
     * TODO: Add JavaDoc
     * @param args
     * @return
     * @throws Exception
     */
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Job job = new Job(getConf());
        job.setJarByClass(TwitterAnalysisJob.class);
        job.setJobName("TweetsAnalysis");

        job.setInputFormatClass(TextInputFormat.class); // TweetInputFormat
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setPartitionerClass(TweetKeyPartitioner.class);

        job.setMapperClass(TweetCountMapper.class);
        job.setReducerClass(TweetCountReducer.class);

        job.setMapOutputKeyClass(TweetKey.class);
        job.setMapOutputValueClass(MappedTweet.class);

        job.setGroupingComparatorClass(TweetKeyGroupComparator.class);
        job.setSortComparatorClass(TweetKeySortComparator.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setNumReduceTasks(2);

        this.setFileDirectories(job, args);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void setFileDirectories(Job job, String[] args) throws IOException {
//        Path inputPath = args[0] == null ? new Path("/root/Documents/cache-1000000-json") : new Path(args[0]);
//        Path outputPath = args[1] == null ? new Path("output_tweets") : new Path(args[1]);
        Path inputPath = new Path("/root/Documents/cache-1000000-json");
        Path outputPath = new Path("output_tweets");
        outputPath.getFileSystem(getConf()).delete(outputPath,true);

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);
    }
}
