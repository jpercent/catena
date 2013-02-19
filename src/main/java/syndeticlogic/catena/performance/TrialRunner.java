package syndeticlogic.catena.performance;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TrialRunner {
    private static final Log log = LogFactory.getLog(TrialRunner.class);
    private final TrialMonitor monitor;
    private final IOController[] controllers;
    private final TrialResultCollector results;
    private final Thread[] runThreads;
    
    public TrialRunner(TrialMonitor monitor, IOController[] controllers, TrialResultCollector results) {
        this.monitor = monitor;
        this.controllers = controllers;
        this.results = results;
        runThreads = new Thread[controllers.length];
    }
    
    void startTrial() {
        monitor.start();
        for(int i = 0; i < controllers.length; i++) {
            runThreads[i] = startThread(controllers[i]);
        }
    }
    
    Thread startThread(final IOController controller) {
        Thread runThread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    final long start = System.currentTimeMillis();
                    int count = 0;
                    IORecord[] iodescriptors = new IORecord[1024];
                    while (controller.notDone()) {
                        IOExecutor ioexe = controller.getNextIOExecutor();
                        ioexe.performIO();
                        iodescriptors[count] = ioexe.getIORecord();
                        count++;
                        if (count == 1024) {
                            results.addIORecords(controller.getId(), iodescriptors);
                            count = 0;
                        }
                    }
                    final long end = System.currentTimeMillis();
                    long duration = end - start;
                    if(count != 0) {
                        IORecord[] truncated = new IORecord[count];
                        System.arraycopy(iodescriptors, 0, truncated, 0, count);
                        results.addIORecords(controller.getId(), truncated);
                    }                    
                    results.completeTrial(controller.getId(), duration);
                } catch (Throwable t) {
                    log.error("Exception in TrialRunnder: ", t);
                    throw new RuntimeException(t);
                }
            }
        });
        runThread.start();
        return runThread;
    }

    void waitForTrialCompletion() throws InterruptedException {
        for(int i = 0; i < runThreads.length; i++) {
            runThreads[i].join();
        }
        monitor.finish();
    }
}
