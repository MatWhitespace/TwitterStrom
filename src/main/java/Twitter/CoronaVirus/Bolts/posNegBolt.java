package Twitter.CoronaVirus.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

public class posNegBolt extends BaseWindowedBolt {
    private float positive,negative;
    private PrintWriter pw;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector){
        positive = negative = 0;
        try {
            File f = new File("/home/matteo/Scrivania/Result.txt");//Se siamo client nfs: '/mnt/public/'
            f.setWritable(true,false);
            pw = new PrintWriter(f);
        } catch (FileNotFoundException e) {
            System.err.println("Errore nella creazione del file");
        }
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple t : tupleWindow.get()){
            float sentiment = t.getFloatByField("rank");
            if (sentiment>0) positive+=sentiment;
            else negative-=sentiment;
        }
        float tot = positive+negative;
        float pos = positive/tot*100;
        float neg = negative/tot*100;
        pw.println("============NEW============");
        pw.printf("positive = %.1f%%\tnegative = %.1f%%\n",pos,neg);
        pw.flush();
    }

    @Override
    public void cleanup() {
        pw.println("\nFinish");
        pw.close();
        super.cleanup();
    }
}
