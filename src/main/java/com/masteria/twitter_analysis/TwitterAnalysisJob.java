package com.masteria.twitter_analysis;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;

public class TwitterAnalysisJob extends Configured implements Tool {

    /**
     *
     * @param arg0
     * @return
     * @throws Exception
     */
    public int run(String[] arg0) throws Exception {
        Job job = new Job(getConf());
        job.setJarByClass(TwitterAnalysisJob.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
