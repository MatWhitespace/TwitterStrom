package Twitter.CoronaVirus.Bolts;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;

public class CountBolt extends BaseWindowedBolt {
    private HashMap<String, Long> counter;

    public CountBolt(){
        counter = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple t :tupleWindow.get()) {
            String trend = t.getStringByField("trend");
            if (counter.containsKey(trend))
                counter.put(trend, counter.get(trend));
            else
                counter.put(trend,1L);
        }

        for (String key : counter.keySet()) {
            Long value = counter.get(key);
            if (value <= 30)
                counter.remove(key);
            else
                System.out.println(key + ":\t" +value);
        }
    }
}
