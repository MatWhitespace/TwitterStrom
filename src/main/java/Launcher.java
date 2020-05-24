package main.java;

import main.java.FileHandler.FileManager;
import main.java.GUI.Component.CoronaFrame;
import main.java.GUI.Component.ElectionFrame;
import main.java.GUI.Component.GenericFrame;
import main.java.GUI.Timer.AsyncThread;
import main.java.Twitter.CoronaVirus.Bolts.*;
import main.java.Twitter.CoronaVirus.spouts.TwitterCoronaVirusSpout;
import main.java.Twitter.Election.Bolts.ReportBolt;
import main.java.Twitter.Election.Bolts.SentimentBolt;
import main.java.Twitter.Election.Bolts.VoteBolt;
import main.java.Twitter.Election.Spouts.TwitterElectionSpout;
import main.java.Twitter.Generic.Bolts.GenericCountBolt;
import main.java.Twitter.Generic.Bolts.GenericSentBolt;
import main.java.Twitter.Generic.Spouts.GenericSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.awt.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class Launcher {

    private static String[] getWordsArray(BufferedReader file) throws IOException{
        StringBuilder sb = new StringBuilder();
        String temp;
        while ((temp = file.readLine()) != null)
            sb.append(temp+",");
        file.close();
        return sb.toString().split(",");
    }


    public static void main(String[] args) throws Exception {

        TopologyBuilder ElectionTopology = makeElection();
        TopologyBuilder CoronaVirusTopology = makeCorona();
        TopologyBuilder GenericTopology = makeGeneric();

        Config conf = new Config();
        conf.setDebug(true);
        int fourGB = 4 * 1024;
        conf.put(Config.TOPOLOGY_WORKER_MAX_HEAP_SIZE_MB, fourGB);

        StormSubmitter.submitTopology("Election", conf, ElectionTopology.createTopology());

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Generic", conf, GenericTopology.createTopology());
        cluster.submitTopology("Corona", conf, CoronaVirusTopology.createTopology());

    }

    private static TopologyBuilder makeGeneric() {
        final String rootPath = "/srv/nfs4/";
        final FileManager fm = new FileManager(rootPath+"nERResult.txt");
        final int campionamento = 1;

        GenericFrame genericFrame = new GenericFrame(fm);

        AsyncThread timer = new AsyncThread(campionamento);
        timer.addComponent(genericFrame);

        EventQueue.invokeLater(() -> {
            genericFrame.init();
        });

        timer.start();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new GenericSpout());

        builder.setBolt("pipeline", new GenericSentBolt()).shuffleGrouping("spout", "lang");
        builder.setBolt("count", new GenericCountBolt(fm).withWindow(new BaseWindowedBolt.Count(10), new BaseWindowedBolt.Count(10))).shuffleGrouping("pipeline", "sentOutput");

        return builder;
    }

    private static TopologyBuilder makeCorona() {
        String[] positive=null, negative=null, stop=null;
        try{
            stop = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/stop-words.txt")));
            positive = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/positive-words.txt")));
            negative = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/negative-words.txt")));
        }catch (IOException e){
            System.out.println("Errore file main");
        }

        final String rootPath = "/srv/nfs4/";
        final String[] countName = {"trend","verified","state","city"};
        final FileManager[] fm = {
                new FileManager(rootPath+countName[0]+".txt"),
                new FileManager(rootPath+countName[1]+".txt"),
                new FileManager(rootPath+countName[2]+".txt"),
                new FileManager(rootPath+countName[3]+".txt"),
                new FileManager(rootPath+"SentResult.txt"),
                new FileManager(rootPath+"RetweetCount.txt")
        };
        final int campionamento = 1;

        CoronaFrame coronaFrame = new CoronaFrame(fm,campionamento);

        AsyncThread asyncThread = new AsyncThread(campionamento);
        asyncThread.addComponent(coronaFrame);

        EventQueue.invokeLater(() ->{
            coronaFrame.init();
        });

        asyncThread.start();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("spout", new TwitterCoronaVirusSpout());

        builder.setBolt("split", new SentBolt(stop)).shuffleGrouping("spout", "sent");
        builder.setBolt("trend", new CountBolt(countName[0],fm[0]).withWindow(new BaseWindowedBolt.Count(50), new BaseWindowedBolt.Count(50))).shuffleGrouping("split", "trendStream");
        builder.setBolt("trendVer", new CountBolt(countName[1], fm[1]).withWindow(new BaseWindowedBolt.Count(50), new BaseWindowedBolt.Count(50))).shuffleGrouping("split", "verifiedStream");

        builder.setBolt("sentiment", new SentimentBolt(positive,negative)).shuffleGrouping("spout", "sent");
        builder.setBolt("report", new posNegBolt(fm[4]).withWindow(new BaseWindowedBolt.Count(50), new BaseWindowedBolt.Count(50))).shuffleGrouping("sentiment", "coronaStream");

        builder.setBolt("retweet", new RetweetBolt(fm[5]).withWindow(new BaseWindowedBolt.Count(500), new BaseWindowedBolt.Count(500))).shuffleGrouping("spout", "ret");

        builder.setBolt("place", new PlaceBolt()).shuffleGrouping("spout","tot");
        builder.setBolt("cityCount", new CountBolt(countName[3],fm[3]).withWindow(new BaseWindowedBolt.Count(50), new BaseWindowedBolt.Count(50))).shuffleGrouping("place","cityStream");
        builder.setBolt("stateCount", new CountBolt(countName[2],fm[2]).withWindow(new BaseWindowedBolt.Count(50), new BaseWindowedBolt.Count(50))).shuffleGrouping("place","stateStream");

        return builder;
    }

    private static TopologyBuilder makeElection() {
        String[] positive=null, negative=null;

        try{
            positive = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/positive-words.txt")));
            negative = getWordsArray(new BufferedReader(new FileReader("/srv/nfs4/negative-words.txt")));
        }catch (IOException e){
            System.out.println("Errore file main");
        }

        String fileName = "/srv/nfs4/Result.txt";
        FileManager fm = new FileManager(fileName);
        final int campionamento = 1;

        ElectionFrame electionFrame = new ElectionFrame(fm, campionamento);

        AsyncThread timer = new AsyncThread(campionamento);
        timer.addComponent(electionFrame);

        EventQueue.invokeLater(() ->{
            electionFrame.init();
        });

        timer.start();

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("TrumpTweets", new TwitterElectionSpout(("Trump")));
        builder.setSpout("BidenTweets", new TwitterElectionSpout(("Biden")));

        builder.setBolt("Sentiment", new SentimentBolt(positive,negative)).allGrouping("TrumpTweets","electionStream").allGrouping("BidenTweets","electionStream");
        builder.setBolt("Vote", new VoteBolt()).allGrouping("Sentiment","electionStream");

        builder.setBolt("Report", new ReportBolt(fm).withWindow(new BaseWindowedBolt.Count(10), new BaseWindowedBolt.Count(10))).allGrouping("Vote","electionStream");

        return builder;

    }
}
