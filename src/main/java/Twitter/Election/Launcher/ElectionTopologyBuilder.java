package Twitter.Election.Launcher;

import Twitter.Election.Bolts.ReportBolt;
import Twitter.Election.Bolts.SentimentBolt;
import Twitter.Election.Bolts.VoteBolt;
import Twitter.Election.Spouts.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;

import java.util.concurrent.TimeUnit;

public class ElectionTopologyBuilder {

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TrumpTweets", new TwitterSpout(("Trump")));
        builder.setSpout("BidenTweets", new TwitterSpout(("Biden")));

        builder.setBolt("Sentiment", new SentimentBolt()).allGrouping("TrumpTweets").allGrouping("BidenTweets");
        builder.setBolt("Vote", new VoteBolt()).allGrouping("Sentiment");

        builder.setBolt("Report", new ReportBolt().withWindow(new Count(30), new Count(30))).allGrouping("Vote");

        Config conf = new Config();
        conf.setDebug(false);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Election", conf, builder.createTopology());
        TimeUnit.MINUTES.sleep(3);
        cluster.killTopology("Election");
        TimeUnit.MINUTES.sleep(1);
        cluster.shutdown();
    }
}
