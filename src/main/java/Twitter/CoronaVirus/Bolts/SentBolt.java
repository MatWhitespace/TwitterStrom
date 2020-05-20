package Twitter.CoronaVirus.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.LinkedList;
import java.util.Map;

public class SentBolt extends BaseRichBolt {
    private OutputCollector collector;
    private LinkedList<String> stopWords;

    public SentBolt(String[] StopWords){
        this.stopWords = new LinkedList<>();
        for (String word: StopWords)
            stopWords.add(word);

    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("sent")) {
            boolean verified = tuple.getBooleanByField("verified");
            String tweet = tuple.getStringByField("tweet");
            //tweet.replaceAll("@[A-Za-z0-9]+", " ");
            //tweet.replaceAll("^\\W", " ");
            String[] tweetPieces = tweet.split("[\\W | \\d]");
            for (int i = 0; i < tweetPieces.length; i++)
                if (!stopWords.contains(tweetPieces[i].toLowerCase()))
                    if (verified)
                        collector.emit("verifiedStream", new Values(tweetPieces[i].toUpperCase()));
                    else
                        collector.emit("trendStream", new Values(tweetPieces[i].toUpperCase()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("trendStream",new Fields("trend"));
        outputFieldsDeclarer.declareStream("verifiedStream",new Fields("verified"));
    }
}
