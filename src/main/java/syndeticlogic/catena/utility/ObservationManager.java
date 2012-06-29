package syndeticlogic.catena.utility;

import java.util.List;
import java.util.LinkedList;

public class ObservationManager implements Observer {
    private List<Observer> observers;
    
    public ObservationManager(List<Observer> observers) {
        this.observers = observers;
        if(this.observers == null) {
            this.observers = new LinkedList<Observer>();
        }
    }
    
    public void register(Observer observer) {
        this.observers.add(observer);
    }
    
    @Override
    public void notify(Observeable obserable) {
        for(Observer observer : observers) 
            observer.notify(obserable);
    }
}
