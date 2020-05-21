package Twitter.CoronaVirus.Bolts;

import FileHandler.FileManager;

import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.FileNotFoundException;
import java.io.PrintWriter;


public class posNegBolt extends BaseWindowedBolt {
    private float positive,negative;
    private FileManager fm;

    public posNegBolt(FileManager fm){
        this.fm =fm;
        positive = negative = 0;
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

        PrintWriter pw = null;
        try{
            pw = fm.getWrite();
            pw.println(pos+"\t"+neg);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally{
            fm.stopWrite(pw);
        }
    }

}
