package Twitter.CoronaVirus.Bolts;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.*;

import static java.util.stream.Collectors.toList;

public class RetweetBolt extends BaseWindowedBolt {
    private HashMap<Long,String> tweet;
    private HashMap<Long,Integer> count;
    private LocalDateTime tomorrowMidnight;

    public RetweetBolt(){
        tweet = new HashMap<>();
        count = new HashMap<>();
        LocalDateTime todayMidnight = LocalDateTime.of(LocalDate.now(ZoneId.of("Europe/Rome")), LocalTime.MIDNIGHT);
        LocalDateTime tomorrowMidnight = todayMidnight.plusDays(1);
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

        for(long key : sorted)
            System.out.println(tweet.get(key));

        LocalDateTime now = LocalDateTime.now();
        if(now.isAfter(tomorrowMidnight)){
            count = new HashMap<>();
            tweet = new HashMap<>();
            tomorrowMidnight = tomorrowMidnight.plusDays(1);
        }
    }

}
