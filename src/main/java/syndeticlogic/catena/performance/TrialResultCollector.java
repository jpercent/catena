package syndeticlogic.catena.performance;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TrialResultCollector {
    ConcurrentMap<IOController, TrialDescriptor> trials;
    
    public TrialResultCollector() {
        trials = new ConcurrentHashMap<IOController, TrialDescriptor>();
    }
    
    public void addIODescriptor(IOController controller, IODescriptor ioDescriptor) {
        if(trials.containsKey(controller)) {
            trials.get(controller).ios.add(ioDescriptor);
        } else {
            TrialDescriptor desc = new TrialDescriptor(controller);
            desc.ios.add(ioDescriptor);
            trials.put(controller, desc);
        }        
    }
    
    public void addIODescriptors(IOController controller, IODescriptor...ioDescriptor) {
        if(trials.containsKey(controller)) {
            trials.get(controller).ios.addAll(Arrays.asList(ioDescriptor));
        } else {
            TrialDescriptor desc = new TrialDescriptor(controller);
            desc.ios.addAll(Arrays.asList(ioDescriptor));
            trials.put(controller, desc);
        }        
    }
    
    public void completeTrial(IOController controller, long duration) {
        if(trials.containsKey(controller)) {
            trials.get(controller).duration = duration;
        } else {
            throw new RuntimeException("attempted to complete a trial that didn't create any records");
        }        
    }
    
    public Map<IOController, TrialDescriptor> getTrials() {
        return trials;
    }
    
    public class TrialDescriptor {
        IOController controller;
        List<IODescriptor> ios;
        long duration;
        
        public TrialDescriptor(IOController controller) {
            this.controller = controller;
            this.ios = new LinkedList<IODescriptor>();
        }
    }
}
