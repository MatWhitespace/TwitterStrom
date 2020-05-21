package Twitter.CoronaVirus.Bolts;


import com.byteowls.jopencage.JOpenCageGeocoder;
import com.byteowls.jopencage.model.JOpenCageResponse;
import com.byteowls.jopencage.model.JOpenCageReverseRequest;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class PlaceBolt extends BaseRichBolt {
    private OutputCollector collector;
    private JOpenCageGeocoder jOpenCageGeocoder;

    @Override
    public void prepare(Map<String, Object> map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
        jOpenCageGeocoder = new JOpenCageGeocoder("0dea83d0785f420f95a33f8f912bb038");
    }

    private String[] decode(double latitude, double longitude){
        JOpenCageReverseRequest request = new JOpenCageReverseRequest(latitude,longitude);
        request.setLanguage("en"); // prioritize results in a specific language using an IETF format language code
        request.setNoDedupe(true); // don't return duplicate results
        request.setLimit(5); // only return the first 5 results (default is 10)
        request.setNoAnnotations(true); // exclude additional info such as calling code, timezone, and currency
        request.setMinConfidence(3); // restrict to results with a confidence rating of at least 3 (out of 10)
        JOpenCageResponse response = jOpenCageGeocoder.reverse(request);

        String[] res = response.getResults().get(0).getFormatted().split(",");
        String[] result = new String[2];
        result[0] = res[res.length-2];
        result[1] = res[res.length-1];
        return result;
    }

    @Override
    public void execute(Tuple tuple) {
        if (tuple.getSourceStreamId().equals("tot")) {
            Double latitude = tuple.getDoubleByField("latitude");
            Double longitude = tuple.getDoubleByField("longitude");
            String[] place = decode(latitude, longitude);
            collector.emit("cityStream", new Values(place[0]));
            collector.emit("stateStream", new Values(place[1]));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("cityStream",new Fields("city"));
        outputFieldsDeclarer.declareStream("stateStream", new Fields("state"));
    }
}
