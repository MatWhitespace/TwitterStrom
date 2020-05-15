package Control.Subjects;

import java.util.HashMap;
import java.util.List;

public class GenStormSubject extends AbstarctSubject{
    private HashMap<String, List<String>> state = new HashMap<>();

    public HashMap<String, List<String>> getState() {
        return state;
    }

    public void setState(HashMap<String,List<String>> stateIN) {
        for (String key : stateIN.keySet()){
            state.put(key, stateIN.get(key));
        }
        notifyObservers("Generic");
    }

}
