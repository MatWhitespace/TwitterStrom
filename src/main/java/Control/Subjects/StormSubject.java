package Control.Subjects;

import java.util.HashMap;
import java.util.List;


public class StormSubject extends AbstarctSubject{
    private HashMap<String, List<String>> state = new HashMap<>();

    public List<String> getState(String key){
        return state.get(key);
    }

    public void setState(HashMap<String,List<String>> stateIN) {
        for (String key : stateIN.keySet()){
            state.put(key, stateIN.get(key));
            notifyObservers(key);
        }
    }
}
