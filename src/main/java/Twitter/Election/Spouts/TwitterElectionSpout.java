package main.java.Twitter.Election.Spouts;

import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageForwardRequest;
import com.byteowls.jopencage.model.JOpenCageResponse;
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

public class TwitterElectionSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private TwitterStream twitter;
    private ArrayBlockingQueue<Status> queque;
    private String keyword;
    private JOpenCageGeocoder jOpenCageGeocoder;

    public TwitterElectionSpout(String keyword){
        this.keyword=keyword;
    }

    private boolean isUSA(String location){
        if(location.equals("null"))
            return false;
        JOpenCageForwardRequest request = new JOpenCageForwardRequest(location);
        request.setLanguage("en"); // prioritize results in a specific language using an IETF format language code
        request.setNoDedupe(true); // don't return duplicate results
        request.setLimit(5); // only return the first 5 results (default is 10)
        request.setNoAnnotations(true); // exclude additional info such as calling code, timezone, and currency
        request.setMinConfidence(3); // restrict to results with a confidence rating of at least 3 (out of 10)

        JOpenCageResponse response = jOpenCageGeocoder.forward(request);

        String[] formatted = response.getResults().get(0).getFormatted().split(",");
        return formatted[formatted.length-1].trim().equals("United States of America");
    }

    public void open(Map<String, Object> map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector=spoutOutputCollector;
        queque = new ArrayBlockingQueue(100, true);

        jOpenCageGeocoder = new JOpenCageGeocoder("0dea83d0785f420f95a33f8f912bb038");

        ConfigurationBuilder cb = new ConfigurationBuilder()
                .setOAuthConsumerKey("ii6oFK75SHTniv70ALoDnw2vN")
                .setOAuthConsumerSecret("Gd6CVOhuysjKPpdh8t99OTXZFUMaPNAV0Ma3ePwrEG3Jg5jxLm")
                .setOAuthAccessToken("1235255325869174788-iDQ6HJ8E4PcZv32h3Z228TAl57Q4w5")
                .setOAuthAccessTokenSecret("Gu14r8wj06nXaqwtEkPrdDJ8e9YqlDbjttcvoLwbafNob");
        TwitterStreamFactory tf = new TwitterStreamFactory(cb.build());
        twitter = tf.getInstance();
        twitter.addListener(new StatusAdapter() {
            public void onStatus(Status status) {
                if (!status.isRetweet() && status.getLang().equals("en") && isUSA(status.getUser().getLocation()))
                    queque.add(status);
            }
        });
        FilterQuery query = new FilterQuery();
        query.track(keyword,"USA","vote","Election");
        twitter.filter(query);
    }

    public void nextTuple() {
        Status s = queque.poll();
        if (s == null) {
            try {
                Thread.currentThread().sleep(10000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            collector.emit("electionStream",new Values(s.getUser(),s.getUser().getFollowersCount(),s.getText(), keyword));
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("electionStream",new Fields("User","Followers","tweet","Candidate"));
    }

    public void close(){
        twitter.shutdown();
        super.close();
    }
}
