package Twitter.Generic.Bolts;

import edu.stanford.nlp.pipeline.*;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Properties;

public class GenericSentBolt extends BaseRichBolt {
    private OutputCollector collector;
    private StanfordCoreNLP pipeline;
    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;

        Properties prop = new Properties();
        prop.setProperty("annotators", "tokenize,ssplit,pos,lemma,ner");

        prop.setProperty("ner.fine.regexner.ignorecase", "true");
        prop.setProperty("ner.applyFineGrained", "false");
        prop.setProperty("ner.statisticalOnly", "true");

        pipeline = new StanfordCoreNLP(prop);

    }

    @Override
    public void execute(Tuple tuple) {
        String tweet = tuple.getStringByField("tweet");
        System.err.println("Ciao"); //non arriva
        CoreDocument tweetAnn = new CoreDocument(tweet);
        pipeline.annotate(tweetAnn);
        for (CoreEntityMention em : tweetAnn.entityMentions()) {
            collector.emit("sentOutput", new Values(em.text(), em.entityType()));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("sentOutput", new Fields("text","type"));
    }
}
