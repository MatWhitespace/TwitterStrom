package Twitter.Election.Launcher;

import Twitter.Election.Bolts.ReportBolt;
import Twitter.Election.Bolts.SentimentBolt;
import Twitter.Election.Bolts.VoteBolt;
import Twitter.Election.Spouts.TwitterElectionSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class ElectionTopologyBuilder {

    public static void main(String[] args) throws Exception {
        boolean local=true;
        BufferedReader positive=null, negative=null;
        try{
            positive = new BufferedReader(new FileReader("/srv/nfs4/positive-words.txt"));
            negative = new BufferedReader(new FileReader("/srv/nfs4/negative-words.txt"));
        }catch (IOException e){
            System.out.println("Errore file main");
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TrumpTweets", new TwitterElectionSpout(("Trump")), 1);
        builder.setSpout("BidenTweets", new TwitterElectionSpout(("Biden")), 1);

        builder.setBolt("Sentiment", new SentimentBolt(positive,negative),1).allGrouping("TrumpTweets").allGrouping("BidenTweets");
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
