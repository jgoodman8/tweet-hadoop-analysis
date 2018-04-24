package com.masteria.twitter_analysis.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * This class defines a object used to map a JSON object and get only the properties defined on this class (id, user,
 * number of re-tweets, and username)
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Tweet {

    @JsonProperty("id")
    private String id;

    private String user;

    @JsonProperty("created_at")
    private String createdAt;

    @JsonProperty("retweet_count")
    private long reTweets;

    private SimpleDateFormat dateFormat = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy");

    public String getUser() {
        return user;
    }

    public long getReTweets() {
        return reTweets;
    }

    public long getTimestamp() {
        try {
            Date parseDate = this.dateFormat.parse(this.createdAt);
            return parseDate.getTime();
        } catch (ParseException exception) {
            System.out.println(exception.getMessage());
            return 0;
        }
    }

    @Override
    public String toString() {
        return "Tweet{" +
                "id='" + id + '\'' +
                ", user='" + user + '\'' +
                ", createdAt='" + createdAt + '\'' +
                ", reTweets=" + reTweets +
                '}';
    }

    @JsonProperty("user")
    private void getScreenNameFromUserObject(Map<String, String> stringMap) {
        this.user = stringMap.get("screen_name");
    }
}
