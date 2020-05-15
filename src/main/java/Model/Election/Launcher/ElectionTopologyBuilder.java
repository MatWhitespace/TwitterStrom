package Model.Election.Launcher;

import Model.Election.Bolts.ReportBolt;
import Model.Election.Bolts.SentimentBolt;
import Model.Election.Bolts.VoteBolt;
import Model.Election.Spouts.TwitterElectionSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt.Count;

import java.io.*;
import java.util.concurrent.TimeUnit;

public class ElectionTopologyBuilder {

    private static String[] getWordsArray(BufferedReader file) throws IOException{
        StringBuilder sb = new StringBuilder();
        String temp;
        while ((temp = file.readLine()) != null)
            sb.append(temp+",");
        file.close();
        return sb.toString().split(",");
    }

    public static void main(String[] args) throws Exception {
        boolean local=true;
        String[] positive=null, negative=null;
        try{
            positive = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/positive-words.txt")));
            negative = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/negative-words.txt")));
        }catch (IOException e){
            System.out.println("Errore file main");
        }

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TrumpTweets", new TwitterElectionSpout(("Trump")), 1);
        builder.setSpout("BidenTweets", new TwitterElectionSpout(("Biden")), 1);

        builder.setBolt("Sentiment", new SentimentBolt(positive,negative),1).allGrouping("TrumpTweets","electionStream").allGrouping("BidenTweets","electionStream");
        builder.setBolt("Vote", new VoteBolt(), 1).allGrouping("Sentiment","electionStream");

        builder.setBolt("Report", new ReportBolt().withWindow(new Count(10), new Count(10)), 1).allGrouping("Vote","electionStream");

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
            System.setProperty("storm.jar", "/home/matteo/IdeaProjects/TwitterStrom/target/TwitterStorm-1.0-SNAPSHOT.jar");
            StormSubmitter.submitTopology("Election", conf, builder.createTopology());
        }
    }
}
