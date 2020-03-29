package Twitter.Election.Launcher;

import Twitter.Election.Bolts.ReportBolt;
import Twitter.Election.Bolts.SentimentBolt;
import Twitter.Election.Bolts.VoteBolt;
import Twitter.Election.Spouts.TwitterSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;

import java.util.concurrent.TimeUnit;

public class ElectionTopologyBuilder {

    public static void main(String[] args) throws Exception {
        boolean local=false;
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TrumpTweets", new TwitterSpout(("Trump")), 1);
        builder.setSpout("BidenTweets", new TwitterSpout(("Biden")), 1);

        builder.setBolt("Sentiment", new SentimentBolt(),1).allGrouping("TrumpTweets").allGrouping("BidenTweets");
        builder.setBolt("Vote", new VoteBolt(), 1).allGrouping("Sentiment");

        builder.setBolt("Report", new ReportBolt().withWindow(new Count(30), new Count(30)), 1).allGrouping("Vote");

        Config conf = new Config();
        conf.setDebug(false);
        if (local) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("Election", conf, builder.createTopology());
            TimeUnit.MINUTES.sleep(3);
            cluster.killTopology("Election");
            TimeUnit.MINUTES.sleep(1);
            cluster.shutdown();
        }else {
            System.setProperty("storm.jar", "/home/matteo/IdeaProjects/TwitterStorm/target/TwitterStorm-1.0-SNAPSHOT.jar");
            StormSubmitter.submitTopology("Election", conf, builder.createTopology());
        }
    }
}
