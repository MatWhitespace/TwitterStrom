package Twitter.CoronaVirus.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;

public class CountBolt extends BaseWindowedBolt {
    private ConcurrentHashMap<String, Long> counter;
    private String name;
    //private PrintWriter pw;
    private LocalDateTime tomorrowMidnight;

    public CountBolt(String name){
        this.name = name;
    }

    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        counter = new ConcurrentHashMap<>();
        /*try{
            File f = new File(name+"Result.txt");
            f.setWritable(true,false);
            pw = new PrintWriter(f);
        }catch (IOException e){
            System.err.printf("errore scrittura file Count");
        }*/

        LocalDateTime todayMidnight = LocalDateTime.of(LocalDate.now(ZoneId.of("Europe/Rome")), LocalTime.MIDNIGHT);
        tomorrowMidnight = todayMidnight.plusDays(1);
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
                .limit(10)
                .map(x -> x.getKey())
                .collect(toList());

        HashMap<String, List<String>> result = new HashMap<>();
        result.put(name+"Count",sorted);
        /*
        for (String key : sorted) {
                pw.println(key + ":\t" +counter.get(key));
        }
        pw.println();
        */
        LocalDateTime now = LocalDateTime.now();
        if(now.isAfter(tomorrowMidnight)){
            counter = new ConcurrentHashMap<>();
            tomorrowMidnight = tomorrowMidnight.plusDays(1);
        }
    }

    @Override
    public void cleanup() {
        /*pw.println("\nFinish");
        pw.close();*/
        super.cleanup();
    }
}
