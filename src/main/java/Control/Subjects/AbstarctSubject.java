package Control.Subjects;

import Control.Observers.Observer;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class AbstarctSubject implements Serializable {
    private Map<String, Observer> observers = new HashMap<>();

    public void addObserver(String name,Observer observer) {
        observers.put( name, observer );
    }

    public void removeObserver(String key) {
        observers.remove( key );
    }

    public void notifyObservers(String name) {
        observers.get(name).update();
    }

}
