package com.masteria.twitter_analysis.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {

    @JsonProperty("id")
    private String id;

    private String user;

    @JsonProperty("created_at")
    private String timestamp;

    @JsonProperty("retweet_count")
    private long reTweets;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public long getReTweets() {
        return reTweets;
    }

    public void setReTweets(long reTweets) {
        this.reTweets = reTweets;
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "id='" + id + '\'' +
                ", user='" + user + '\'' +
                ", timestamp='" + timestamp + '\'' +
                ", reTweets=" + reTweets +
                '}';
    }

    @JsonProperty("user")
    private void getScreenNameFromUserObject(Map<String, String> brand) {
        this.setUser(brand.get("screen_name"));
    }
}
