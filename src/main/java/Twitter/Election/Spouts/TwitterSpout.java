package Twitter.Election.Spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;

public class TwitterSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private Twitter twitter;

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector=spoutOutputCollector;

        ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true)
                .setOAuthConsumerKey("ii6oFK75SHTniv70ALoDnw2vN")
                .setOAuthConsumerSecret("Gd6CVOhuysjKPpdh8t99OTXZFUMaPNAV0Ma3ePwrEG3Jg5jxLm")
                .setOAuthAccessToken("1235255325869174788-RzMdFRpm3TAQPd2ll94YaZMpdBhBlp")
                .setOAuthAccessTokenSecret("9JN13Et2xWt82KdCm7jBvxuvHJ1xersyU1dN7J9pQj0xB");
        TwitterFactory tf = new TwitterFactory(cb.build());
        twitter = tf.getInstance();
    }

    public void nextTuple() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
