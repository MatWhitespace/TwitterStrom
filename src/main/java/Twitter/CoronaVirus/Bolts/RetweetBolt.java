package Twitter.CoronaVirus.Bolts;

import FileHandler.FileManager;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;

public class RetweetBolt extends BaseWindowedBolt {
    private ConcurrentHashMap<Long,String> tweet;
    private ConcurrentHashMap<Long,Integer> count;
    private LocalDateTime tomorrowMidnight;
    private FileManager fm;


    public RetweetBolt(FileManager fm){
        this.fm = fm;
        this.tweet = new ConcurrentHashMap<>();
        this.count = new ConcurrentHashMap<>();
        LocalDateTime todayMidnight = LocalDateTime.of(LocalDate.now(ZoneId.of("Europe/Rome")), LocalTime.MIDNIGHT);
        tomorrowMidnight = todayMidnight.plusDays(1);
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple t : tupleWindow.get()){
            if(t.getSourceStreamId().equals("ret")){
                Long id = t.getLongByField("id");
                String text = t.getStringByField("tweet");
                Integer retCount = t.getIntegerByField("retCount");
                count.put(id,retCount);
                tweet.putIfAbsent(id,text);
            }
        }
        List<Long> sorted = count.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(10)
                .map(x -> x.getKey())
                .collect(toList());

        PrintWriter pw = null;
        try{
            pw = fm.getWrite();
            for(long key : sorted)
                pw.println(tweet.get(key).replaceAll(System.lineSeparator(),""));
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally {
            fm.stopWrite(pw);
        }


        LocalDateTime now = LocalDateTime.now();
        if(now.isAfter(tomorrowMidnight)){
            count = new ConcurrentHashMap<>();
            tweet = new ConcurrentHashMap<>();
            tomorrowMidnight = tomorrowMidnight.plusDays(1);
        }
    }

}
