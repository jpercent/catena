package syndeticlogic.catena.performance;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class TrialResultCollector {
    final HashMap<Long, IOControllerResultDescriptor> trials;
    final Thread serializer;
    final ReentrantLock lock;
    final Condition condition;
    boolean done;

    public TrialResultCollector() {
        trials = new HashMap<Long, IOControllerResultDescriptor>();
        lock = new ReentrantLock();
        condition = lock.newCondition();
        done = false;
        serializer = new Thread(new Runnable() {
            @Override
            public void run() {
                lock.lock();
                try {
                    while (!done) {
                        try {
                            condition.await();

                        } catch (InterruptedException e) {

                            e.printStackTrace();
                        }
                    }
                } finally {
                    lock.unlock();
                }
            }
        });
    }
    public void createIOControllerResultDescriptor(Long controllerId, )
    public void addIORecord(IORecord ioDescriptor) {
        lock.lock();
        try {
            if (trials.containsKey(ioDescriptor.getControllerId())) {
                trials.get(ioDescriptor.getControllerId()).ios.add(ioDescriptor);
            } else {
                IOControllerResultDescriptor desc = new IOControllerResultDescriptor();
                desc.ios.add(ioDescriptor);
                trials.put(ioDescriptor.getControllerId(), desc);
            }
        } finally {
            lock.unlock();
        }
    }

    public void addIORecords(Long id, IORecord... ioDescriptor) {
        lock.lock();
        try {
            if (trials.containsKey(id)) {
                trials.get(id).ios.addAll(Arrays.asList(ioDescriptor));
            } else {
                IOControllerResultDescriptor desc = new IOControllerResultDescriptor();
                desc.ios.addAll(Arrays.asList(ioDescriptor));
                trials.put(id, desc);
            }
        } finally {
            lock.unlock();
        }
    }

    public void completeTrial(Long controllerId, long duration) {
        lock.lock();
        try {
            if (trials.containsKey(controllerId)) {
                trials.get(controllerId).duration = duration;
            } else {
                throw new RuntimeException("attempted to complete a trial that didn't create any records");
            }
        } finally {
            lock.unlock();
        }
    }

    public class IOControllerResultDescriptor {
        List<IORecord> ios;
        long duration;
        public IOControllerResultDescriptor() {
            this.ios = new LinkedList<IORecord>();
        }
    }
}
