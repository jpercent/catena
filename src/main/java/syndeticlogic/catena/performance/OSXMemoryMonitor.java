package syndeticlogic.catena.performance;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OSXMemoryMonitor extends AbstractMonitor implements MemoryMonitor {
	private static final Log log = LogFactory.getLog(OSXMemoryMonitor.class);
	private LinkedList<Long> freePages;
	private LinkedList<Long> activePages;
	private LinkedList<Long> inactivePages;
	private LinkedList<Long> wiredPages;
	private LinkedList<Long> faultRoutineCalls;
	private LinkedList<Long> copyOnWriteFaults;
	private LinkedList<Long> zeroFilledPages;
	private LinkedList<Long> reactivePages;
	private LinkedList<Long> pageIns;
	private LinkedList<Long> pageOuts;

	public OSXMemoryMonitor() {
		super();
		setCommandAndArgs("vm_stat", "5");
		freePages = new LinkedList<Long>();
		activePages = new LinkedList<Long>();
		inactivePages = new LinkedList<Long>();
		wiredPages = new LinkedList<Long>();	
		faultRoutineCalls = new LinkedList<Long>();
		copyOnWriteFaults = new LinkedList<Long>();
		zeroFilledPages = new LinkedList<Long>();
		reactivePages = new LinkedList<Long>();
		pageIns = new LinkedList<Long>();
		pageOuts = new LinkedList<Long>();
	}
	
	// AbstractMonitor
	@Override
	public void processMonitorOutput(BufferedReader reader) throws IOException {
		reader.readLine();
		reader.readLine();
		reader.readLine();
		while (true) {
			String line = reader.readLine();
			if (line == null) {
				break;
			}
			log.debug(line);
			line = line.trim();
			String[] values = line.split("\\s+");
			assert values.length == 11;
			int i = 0;
			freePages.add(Long.parseLong(values[i++]));
			activePages.add(Long.parseLong(values[i++]));
			inactivePages.add(Long.parseLong(values[i++]));
			activePages.add(Long.parseLong(values[i++]));
			wiredPages.add(Long.parseLong(values[i++]));
			faultRoutineCalls.add(Long.parseLong(values[i++]));
			copyOnWriteFaults.add(Long.parseLong(values[i++]));
			zeroFilledPages.add(Long.parseLong(values[i++]));
			reactivePages.add(Long.parseLong(values[i++]));
			pageIns.add(Long.parseLong(values[i++]));
			pageOuts.add(Long.parseLong(values[i++]));
		}
	}
	
	@Override
	public void dumpData() {
		System.out.println("Free Pages:           "+freePages);
		System.out.println("Active Pages:         "+activePages);
		System.out.println("Inactive Pages:       "+inactivePages);
		System.out.println("Wired Pages:          "+wiredPages);
		System.out.println("Fault routine calls:  "+faultRoutineCalls);
		System.out.println("Copy on write faults: "+copyOnWriteFaults);
		System.out.println("Reactive pages:       "+reactivePages);
		System.out.println("Page ins:             "+pageIns);
		System.out.println("Page outs:            "+pageOuts);
	}
	
	// MemoryMonitor
	@Override
	public double getAverageFreePages() {
		return computeAverage(freePages);
	}
    
	@Override
	public LinkedList<Long> getRawFreePageMeasurements() {
		return freePages;
	}

    @Override
	public double getAverageActivePages() {
		return computeAverage(activePages);
	}
    
    @Override
	public LinkedList<Long> getRawActivePageMeasurements()  {
		return freePages;
	}

    @Override
	public double getAverageInactivePages() {
		return computeAverage(inactivePages);
	}
    
    @Override
	public LinkedList<Long> getRawInactivePagesMeasurements()  {
		return inactivePages;
	}

    @Override
	public double getAverageWiredPages() {
		return computeAverage(wiredPages);
	}
    
    @Override    
	public LinkedList<Long> getRawWiredPagesMeasurements()   {
		return wiredPages;
	}
	
    @Override
    public double getAverageNumberOfFaultRoutineCalls() {
		return computeAverage(faultRoutineCalls);
	}
    
    @Override
	public LinkedList<Long> getRawNumberOfFaultRoutineCallMeasurements()   {
		return faultRoutineCalls;
	}
	
    @Override
    public double getAverageCopyOnWriteFaults() {
		return computeAverage(copyOnWriteFaults);
	}
    
    @Override
	public LinkedList<Long> getRawCopyOnWriteFaultsMeasurements()   {
		return copyOnWriteFaults;
	}

    @Override
    public double getAverageZeroFilledPages() {
		return computeAverage(zeroFilledPages);
	}
    
    @Override
	public LinkedList<Long> getRawZeroFilledPageMeasurements()   {
		return zeroFilledPages;
	} 

    @Override
    public double getAverageReactivatedPages() {
		return computeAverage(reactivePages);
	}
	
    @Override
	public LinkedList<Long> getRawReactivatedPagesMeasurements()   {
		return reactivePages;
	}
    
    @Override
    public double getAveragePageIns() {
		return computeAverage(pageIns);
	}
    
    @Override
    public LinkedList<Long> getRawPageInMeasurements()   {
		return pageIns;
	}
    
    @Override
    public double getAveragePageOuts() {
		return computeAverage(pageOuts);
	}
    
    @Override
	public LinkedList<Long> getRawPageOutsMeasurements()   {
		return pageOuts;
	}
	
	public static void useMemory() {
		byte[] bytes = new byte[1024*1024*100];
		for(int j = 0; j < 1024*1024*100; j++) {
			bytes[j] = (byte)(23 * j);
		}
	}
	
	public static void main(String[] args) throws Throwable {
		try {
			long starttime = System.currentTimeMillis();
			OSXMemoryMonitor mm = new OSXMemoryMonitor();
			System.out.println("Starting..");
			mm.start();
			Thread.sleep(1000);
			if (args.length == 0) {
				while (starttime + 25522 > System.currentTimeMillis()) {
					useMemory();
				}
			} else {
				while (starttime + 25522 > System.currentTimeMillis()) {
					long count1 = 0;
					while (count1 < 10000000000L)
						count1++;
				}
			}
			mm.finish();
			mm.dumpData();
			long duration = mm.getDurationMillis();
			System.out.println("Duration = " + duration);
		} catch (Throwable t) {
			log.error("exception: ", t);
			throw t;
		}
	}
} 
