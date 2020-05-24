package main.java.Twitter.Election.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import twitter4j.User;

import java.util.HashMap;
import java.util.Map;

public class VoteBolt extends BaseRichBolt {
    private OutputCollector collector;
    private HashMap<Long, Integer> userVote;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector=outputCollector;
        userVote=new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        if(tuple.getSourceStreamId().equals("electionStream")) {
            long userId = ((User) tuple.getValue(0)).getId();
            int followers = tuple.getIntegerByField("Followers");
            int rank = tuple.getIntegerByField("Rank");
            int vote = getVote(followers) * rank;
            Integer storico = userVote.get(userId);
            if (storico != null) vote += storico / 2;
            userVote.put(userId, vote);
            collector.emit("electionStream",new Values(tuple.getStringByField("Candidate"), vote));
        }
    }

    private int getVote(int followers) {
        if(followers > 10000000) return 1000;
        if(followers > 1000000) return 700;
        if(followers > 500000) return 350;
        if(followers > 100000) return 150;
        if(followers > 60000) return 75;
        if(followers > 10000) return 35;
        if(followers > 5000) return 20;
        if(followers > 1000) return 12;
        if(followers > 100) return 6;
        return 1;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("electionStream",new Fields("Candidate","Vote"));
    }
}
