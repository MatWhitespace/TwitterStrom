package Twitter.CoronaVirus.Bolts;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.HashMap;

public class CountBolt extends BaseWindowedBolt {
    private HashMap<String, Long> counter;
    private String name;

    public CountBolt(String name){
        counter = new HashMap<>();
        this.name=name;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        String field =null;
        for (Tuple t :tupleWindow.get()) {
            if (t.getSourceStreamId().equals(name+"Stream"))
                field = t.getStringByField(name);
            if (counter.containsKey(field))
                counter.put(field, counter.get(field));
            else
                counter.put(field,1L);
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
