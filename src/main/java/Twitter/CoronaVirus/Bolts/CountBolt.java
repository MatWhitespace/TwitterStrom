package Twitter.CoronaVirus.Bolts;

import FileHandler.FileManager;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;

public class CountBolt extends BaseWindowedBolt {
    private ConcurrentHashMap<String, Long> counter;
    private String name;
    private LocalDateTime tomorrowMidnight;
    private FileManager fm;

    public CountBolt(String name, FileManager fm){
        this.name = name;
        this.fm = fm;
        this.counter = new ConcurrentHashMap<>();
        LocalDateTime todayMidnight = LocalDateTime.of(LocalDate.now(ZoneId.of("Europe/Rome")), LocalTime.MIDNIGHT);
        this.tomorrowMidnight = todayMidnight.plusDays(1);
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        String field =null;
        for (Tuple t :tupleWindow.get()) {
            if (t.getSourceStreamId().equals(name+"Stream"))
                field = t.getStringByField(name);
            if (counter.containsKey(field))
                counter.put(field, counter.get(field)+1);
            else
                counter.put(field, 1L);

        }
        List<String> sorted = counter.entrySet().stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .limit(5)
                .map(x -> x.getKey())
                .collect(toList());

        PrintWriter pw = null;
        try {
            pw = fm.getWrite();
            for (String key : sorted) {
                pw.println(key + "\t" +counter.get(key));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally{
            fm.stopWrite(pw);
        }


        LocalDateTime now = LocalDateTime.now();
        if(now.isAfter(tomorrowMidnight)){
            counter = new ConcurrentHashMap<>();
            tomorrowMidnight = tomorrowMidnight.plusDays(1);
        }
    }

}
