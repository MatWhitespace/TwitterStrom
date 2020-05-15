package Model.Generic.Spouts;

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
    private ArrayBlockingQueue<Status> tweets, langTweets;
    private TwitterStream twitter;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        tweets = new ArrayBlockingQueue(100, true);
        langTweets = new ArrayBlockingQueue(100, true);


        ConfigurationBuilder cb = new ConfigurationBuilder()
                .setOAuthConsumerKey("ii6oFK75SHTniv70ALoDnw2vN")
                .setOAuthConsumerSecret("Gd6CVOhuysjKPpdh8t99OTXZFUMaPNAV0Ma3ePwrEG3Jg5jxLm")
                .setOAuthAccessToken("1235255325869174788-iDQ6HJ8E4PcZv32h3Z228TAl57Q4w5")
                .setOAuthAccessTokenSecret("Gu14r8wj06nXaqwtEkPrdDJ8e9YqlDbjttcvoLwbafNob");
        TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
        twitter = tf.getInstance();
        twitter.addListener(new StatusAdapter() {
            public void onStatus(Status status) {
                if(!status.isRetweet()) {
                    if (status.getLang().equals("en"))
                        langTweets.add(status);
                    tweets.add(status);
                }
            }
        });

        twitter.sample();

    }

    @Override
    public void nextTuple() {
        Status tweet = tweets.poll();
        Status langTweet = langTweets.poll();
        if (tweet == null && langTweet == null) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            if (tweet != null) {
                collector.emit("tot", new Values(tweet));
            }if (langTweet != null) {
                collector.emit("lang", new Values(langTweet.getText()));
            }
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("tot", new Fields("status"));
        outputFieldsDeclarer.declareStream("lang", new Fields("tweet"));
    }
}
