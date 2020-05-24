package main.java.Twitter.Generic.Bolts;

import main.java.FileHandler.FileManager;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.windowing.TupleWindow;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;

public class GenericCountBolt extends BaseWindowedBolt {
    private HashMap<String, TreeSet<String>> nERCollector;
    private FileManager fm;

    public GenericCountBolt(FileManager fm){
        this.nERCollector = new HashMap<>();
        this.fm = fm;
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
        PrintWriter pw = null;
        try{
            pw = fm.getWrite();
            for (String key : nERCollector.keySet()){
                pw.print(key + "\t");
                for (String value : nERCollector.get(key))
                    pw.print(value+"\t");
                pw.println();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }finally{
            fm.stopWrite(pw);
        }


    }
}
