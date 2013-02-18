package syndeticlogic.catena.performance;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MultiMonitorDecorator extends AbstractMonitor implements MemoryMonitor, IOMonitor {
	private Log log = LogFactory.getLog(AbstractMonitor.class);	
	private MemoryMonitor memoryMonitor;
	private IOMonitor ioMonitor;
	
	public MultiMonitorDecorator(MemoryMonitor memoryMonitor, IOMonitor ioMonitor) {
	    super();
	    this.memoryMonitor = memoryMonitor;
	    this.ioMonitor = ioMonitor;
	}

    @Override
	public void start() {
	    memoryMonitor.start();
	    ioMonitor.start();
	}
    
    @Override    	
	public void finish() {
	    memoryMonitor.start();
	    ioMonitor.start();
	}
    
    @Override   
    public void dumpData() {
        log.warn("MultiMonitorDecorator does not implement dumpData");
    }
    
    @Override
    public void processMonitorOutput(BufferedReader reader) throws IOException {
        log.warn("MultiMonitorDecorator does not implement processMonitorOutput");
    }   
    
    @Override
    public double getAverageKilobytesPerTransfer() {
        return ioMonitor.getAverageKilobytesPerTransfer();
    }

    @Override
    public List<Double> getRawKiloBytesPerTranferMeasurements() {
        return ioMonitor.getRawKiloBytesPerTranferMeasurements();
    }

    @Override
    public double getAverageTransfersPerSecond() {
        return ioMonitor.getAverageTransfersPerSecond();
    }

    @Override
    public List<Double> getRawTransfersPerSecond() {
        return ioMonitor.getRawTransfersPerSecond();
    }

    @Override
    public double getAverageMegabytesPerSecond() {
        return ioMonitor.getAverageMegabytesPerSecond();
    }

    @Override
    public List<Double> getRawMegabytesPerSecond() {
        return ioMonitor.getRawMegabytesPerSecond();
    }

    @Override
    public double getAverageUserModeTime() {
        return ioMonitor.getAverageUserModeTime();
    }

    @Override
    public List<Long> getRawUserModeTime() {
        return ioMonitor.getRawUserModeTime();
    }

    @Override
    public double getAverageSystemModeTime() {
        return ioMonitor.getAverageSystemModeTime();
    }

    @Override
    public List<Long> getRawSystemModeTime() {
        return ioMonitor.getRawSystemModeTime();
    }

    @Override
    public double getAverageIdleModeTime() {
        return ioMonitor.getAverageIdleModeTime();
    }

    @Override
    public List<Long> getRawIdleModeTime() {
        return ioMonitor.getRawIdleModeTime();
    }

    @Override
    public double getAverageFreePages() {
        return memoryMonitor.getAverageFreePages();
    }

    @Override
    public LinkedList<Long> getRawFreePageMeasurements() {
        return memoryMonitor.getRawFreePageMeasurements();
    }

    @Override
    public double getAverageActivePages() {
        return memoryMonitor.getAverageActivePages();
    }

    @Override
    public LinkedList<Long> getRawActivePageMeasurements() {
        return memoryMonitor.getRawActivePageMeasurements();
    }

    @Override
    public double getAverageInactivePages() {
        return memoryMonitor.getAverageInactivePages();
    }

    @Override
    public LinkedList<Long> getRawInactivePagesMeasurements() {
        return memoryMonitor.getRawInactivePagesMeasurements();
    }

    @Override
    public double getAverageWiredPages() {
        return memoryMonitor.getAverageWiredPages();
    }

    @Override
    public LinkedList<Long> getRawWiredPagesMeasurements() {
        return memoryMonitor.getRawWiredPagesMeasurements();
    }

    @Override
    public double getAverageNumberOfFaultRoutineCalls() {
        return memoryMonitor.getAverageNumberOfFaultRoutineCalls();
    }

    @Override
    public LinkedList<Long> getRawNumberOfFaultRoutineCallMeasurements() {
        return memoryMonitor.getRawNumberOfFaultRoutineCallMeasurements();
    }

    @Override
    public double getAverageCopyOnWriteFaults() {
        return memoryMonitor.getAverageCopyOnWriteFaults();
    }

    @Override
    public LinkedList<Long> getRawCopyOnWriteFaultsMeasurements() {
        return memoryMonitor.getRawCopyOnWriteFaultsMeasurements();
    }

    @Override
    public double getAverageZeroFilledPages() {
        return memoryMonitor.getAverageZeroFilledPages();
    }

    @Override
    public LinkedList<Long> getRawZeroFilledPageMeasurements() {
        return memoryMonitor.getRawZeroFilledPageMeasurements();
    }

    @Override
    public double getAverageReactivatedPages() {
        return memoryMonitor.getAverageReactivatedPages();
    }

    @Override
    public LinkedList<Long> getRawReactivatedPagesMeasurements() {
        return memoryMonitor.getRawReactivatedPagesMeasurements();
    }

    @Override
    public double getAveragePageIns() {
        return memoryMonitor.getAveragePageIns();
    }

    @Override
    public LinkedList<Long> getRawPageInMeasurements() {
        return memoryMonitor.getRawPageInMeasurements();
    }

    @Override
    public double getAveragePageOuts() {
        return memoryMonitor.getAveragePageOuts();
    }

    @Override
    public LinkedList<Long> getRawPageOutsMeasurements() {
        return memoryMonitor.getRawPageOutsMeasurements();
    }
}