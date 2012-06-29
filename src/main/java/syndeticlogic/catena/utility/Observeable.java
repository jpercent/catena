package syndeticlogic.catena.utility;

public interface Observeable {
    
    public interface State {
    }
    
    State state();
    void state(State state);
}
