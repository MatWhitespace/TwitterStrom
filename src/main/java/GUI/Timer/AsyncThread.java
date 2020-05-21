package GUI.Timer;

import GUI.Component.GuiComponent;
import org.joda.time.Minutes;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AsyncThread extends Thread{
    private TimeUnit timeUnit = TimeUnit.MINUTES;
    private List<GuiComponent> components = new LinkedList<>();
    private boolean run = true;
    final int MINUTES;

    public AsyncThread(final int MINUTES){
        this.MINUTES = MINUTES;
    }

    public void addComponent(GuiComponent component){
        this.components.add(component);
    }

    public void finish(){
        try {
            TimeUnit.MINUTES.sleep(this.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally{
            run = false;
        }
    }

    @Override
    public void run() {
        while (run){
            try {
                TimeUnit.MINUTES.sleep(this.MINUTES);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (GuiComponent component : components) {
                component.updateData();
            }
        }
    }
}
