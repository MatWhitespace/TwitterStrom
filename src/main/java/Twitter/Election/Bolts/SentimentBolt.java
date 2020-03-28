package Twitter.Election.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SentimentBolt extends BaseRichBolt {
    private OutputCollector collector;
    private String[] positiveWords, negativeWords;

    public String[] getWordsArray(String fileName) throws IOException {
        StringBuilder sb = new StringBuilder();
        BufferedReader input = new BufferedReader(new FileReader("/srv/nfs4/"+fileName));//Se siamo client nfs: '/mnt/public/'
        String temp;
        while ((temp = input.readLine()) != null)
            sb.append(temp+",");
        input.close();
        return sb.toString().split(",");
    }

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
        try {
            this.positiveWords = getWordsArray("positive-words.txt");
            this.negativeWords = getWordsArray("negative-words.txt");
        } catch (IOException e) {
            System.err.println("Errore di lettura dei file");
        }
    }

    public float getRank(String tweet) {
        int positiveRank = 0;
        for (int i = 0; i < positiveWords.length; i++) {
            Matcher m = Pattern.compile("\\b" + positiveWords[i] + "\\b").matcher(tweet);
            while (m.find()) {
                positiveRank++;
            }
        }
        int negativeRank = 0;
        for (int i = 0; i < negativeWords.length; i++) {
            Matcher m = Pattern.compile("\\b" + negativeWords[i] + "\\b").matcher(tweet);
            while (m.find()) {
                negativeRank++;
            }
        }
        int den = Integer.max(positiveRank,negativeRank);
        return (den==0)?0:(positiveRank - negativeRank)/den;

    }

    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("Tweet");
        float rank = getRank(tweet);
        int rankField = 0; //voto neutrale
        if (rank > 0.3f) rankField=1; //voto positivo
        else if (rank < -0.3f) rankField=-1; //voto negativo
        if(rankField!=0)
            collector.emit(new Values(tuple.getValue(0), tuple.getValue(1),rankField, tuple.getValue(3)));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("User","Followers","Rank","Candidate"));
    }
}
