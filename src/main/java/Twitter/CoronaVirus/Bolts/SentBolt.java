package Twitter.CoronaVirus.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentBolt extends BaseRichBolt {
    private OutputCollector collector;
    private LinkedList<String> stopWords;

    public SentBolt(BufferedReader bf){
        try{
            stopWords = getWordsList(bf);
        }catch (IOException e){
            System.err.println("Errore lettura file");
        }
    }

    private LinkedList<String> getWordsList(BufferedReader file) throws IOException {
        LinkedList<String> result = new LinkedList<>();
        String temp;
        while ((temp = file.readLine()) != null)
            result.add(temp);
        file.close();
        return result;
    }

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        if (!tuple.getSourceStreamId().equals("sent"))
            return;
        String tweet = tuple.getStringByField("tweet");
        tweet.replaceAll("@[A-Za-z0-9]+"," ");
        tweet.replaceAll("[^a-zA-Z]"," ");
        String[] tweetPieces = tweet.split(" ");
        for (int i =0; i<tweetPieces.length; i++)
            if (!stopWords.contains(tweetPieces[i]))
                collector.emit("sent",new Values(tweetPieces[i]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("trend"));
    }
}
