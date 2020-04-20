package Twitter.CoronaVirus.spouts;

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

public class TwitterCoronaVirusSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private TwitterStream twitter;
    private ArrayBlockingQueue<Status> sentimentTweet, totalTweet, retwetted;

    @Override
    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        sentimentTweet = new ArrayBlockingQueue(100, true);
        totalTweet = new ArrayBlockingQueue(100, true);
        retwetted = new ArrayBlockingQueue(100,true);

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
                        sentimentTweet.add(status);
                    totalTweet.add(status);
                }
                else if (status.getLang().equals("en"))
                    retwetted.add(status);
            }
        });

        FilterQuery query = new FilterQuery();
        query.track("corona","virus");
        twitter.filter(query);
    }

    @Override
    public void nextTuple() {
        Status sent = sentimentTweet.poll();
        Status tot = totalTweet.poll();
        Status ret = retwetted.poll();
        if (sent == null && tot == null && ret == null) {
            try {
                Thread.currentThread().sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            if (sent != null)
                collector.emit("sent",new Values(sent.getText()), sent.getUser().isVerified());
            if (tot != null)
                collector.emit("tot", new Values(tot.getGeoLocation().getLatitude(), tot.getGeoLocation().getLongitude()));
            if (ret != null)
                collector.emit("ret",new Values(ret.getText(), ret.getRetweetCount()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("sent",new Fields("tweet","verified"));
        outputFieldsDeclarer.declareStream("tot",new Fields("latitude","longitude"));
        outputFieldsDeclarer.declareStream("ret",new Fields("tweet","retCount"));
    }
}
