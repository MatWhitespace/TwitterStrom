package main.java.Twitter.Election.Bolts;

import main.java.FileHandler.FileManager;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class ReportBolt extends BaseWindowedBolt {
    private long Trump,Biden;
    private FileManager fm;

    public ReportBolt(FileManager fm){
        Trump = Biden = 0L;
        this.fm = fm;
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

        PrintWriter pw = null;

        try {
            pw = fm.getWrite();//Se siamo client nfs: '/mnt/public/'
        } catch (FileNotFoundException | InterruptedException fe ) {
            System.err.println("Errore nella creazione del file");
        }

        pw.printf("%.1f\t%.1f\n",percBiden,percTrump);
        fm.stopWrite(pw);
    }

}
