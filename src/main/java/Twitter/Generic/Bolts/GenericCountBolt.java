package Twitter.Generic.Bolts;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

public class GenericCountBolt extends BaseWindowedBolt {
    private HashMap<String, TreeSet<String>> nERCollector;

    public GenericCountBolt(){
        this.nERCollector = new HashMap<>();
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for (Tuple t: tupleWindow.get()){
            String type = t.getStringByField("type");
            String text = t.getStringByField("text");
            if (nERCollector.containsKey(type))
                nERCollector.get(type).add(text);
            else {
                TreeSet<String> entity = new TreeSet<>();
                entity.add(text);
                nERCollector.put(type,entity);
            }
        }
        HashMap<String, List<String>> result = new HashMap<>();
        for (String key : nERCollector.keySet()){
            result.put(key,new ArrayList<>(nERCollector.get(key)));
        }

    }
}
