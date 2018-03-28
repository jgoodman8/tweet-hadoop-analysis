package com.masteria.twitter_analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class Application {

    public static void main(String[] args) throws Exception {
        int executionResult;

        try {
            executionResult = ToolRunner.run(new Configuration(), new TwitterAnalysisJob(), args);
            System.exit(executionResult);
        } catch (Exception globalException) {
            globalException.printStackTrace();
        }
    }

}
