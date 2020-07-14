package main.java.Twitter.Generic.Spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

public class GenericSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private ArrayBlockingQueue<Status>  langTweets;
    private TwitterStream twitter;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        langTweets = new ArrayBlockingQueue(100, true);


        ConfigurationBuilder cb = new ConfigurationBuilder()
                .setOAuthConsumerKey("***")
                .setOAuthConsumerSecret("***")
                .setOAuthAccessToken("***")
                .setOAuthAccessTokenSecret("***");
        TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
        twitter = tf.getInstance();
        twitter.addListener(new StatusAdapter() {
            public void onStatus(Status status) {
                if(!status.isRetweet()) {
                    if (status.getLang().equals("en"))
                        langTweets.add(status);
                }
            }
        });

        twitter.sample();

    }

    @Override
    public void nextTuple() {
        Status langTweet = langTweets.poll();
        if (langTweet == null) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            collector.emit("lang", new Values(langTweet.getText()));
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("lang", new Fields("tweet"));
    }
}
