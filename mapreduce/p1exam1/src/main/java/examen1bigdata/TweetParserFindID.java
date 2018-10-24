package examen1bigdata;

import org.apache.htrace.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import org.apache.htrace.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TweetParserFindID {
    String id_str;
    String text = "";
    String extended_tweet_text ="";

    public TweetParserFindID(){

    }

    @SuppressWarnings("unchecked")
    @JsonProperty("extended_tweet")
    private void helper(Map<String,Object> extended_tweet) {
        this.extended_tweet_text = (String)extended_tweet.get("full_text");

    }

    public String getId_str() {
        return id_str;
    }

    public String getText() {
        return text;
    }

    public String getExtended_tweet_text() {
        return extended_tweet_text;
    }
}
