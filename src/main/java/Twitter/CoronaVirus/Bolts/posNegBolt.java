package Twitter.CoronaVirus.Bolts;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

public class posNegBolt extends BaseWindowedBolt {
    private int positive=0,negative=0;

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple t : tupleWindow.get()){
            int sentiment = t.getIntegerByField("rank");
            if (sentiment>0) positive+=sentiment;
            else negative-=sentiment;
        }
        int tot = positive+negative;
        float pos = (float) positive/tot;
        float neg = (float) negative/tot;
        System.out.printf("Positive:\t%.1f\nNegative:\t%.1f",pos,neg);
    }
}
