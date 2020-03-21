package Twitter.Election.Bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

public class ReportBolt extends BaseWindowedBolt {
    private long Trump,Biden;
    private PrintWriter pw;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        Trump = Biden = 0L;
        try {
            File f = new File("/srv/nfs4/Result.txt");
            f.setWritable(true,false);
            pw = new PrintWriter(f);
        } catch (FileNotFoundException e) {
            System.err.println("Errore nella creazione del file");
        }
    }

    @Override
    public void execute(TupleWindow tupleWindow) {
        for(Tuple t : tupleWindow.get()){
            int voto=0;
            if(t.getStringByField("Candidate").equals("Trump")) {
                if ((voto = t.getIntegerByField("Vote")) > 0) Trump += voto;
                else Biden += (Math.abs(voto) / 3);
            }else {
                if ((voto = t.getIntegerByField("Vote")) > 0) Biden += voto;
                else Trump += (Math.abs(voto) / 3);
            }
        }
        Long den = Trump+Biden;
        double percBiden = ((double) Biden/den)*100;
        double percTrump = ((double) Trump/den)*100;
        pw.println("==========NEW==========");
        pw.printf("Biden = %.1f%%\tTrump = %.1f%%\n",percBiden,percTrump);
        pw.flush();
    }

    @Override
    public void cleanup() {
        pw.println("Finish");
        pw.close();
        super.cleanup();
    }
}
