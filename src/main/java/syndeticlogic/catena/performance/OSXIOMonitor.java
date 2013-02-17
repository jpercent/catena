package syndeticlogic.catena.performance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class OSXIOMonitor extends AbstractMonitor implements IOMonitor {
	private static final Log log = LogFactory.getLog(OSXIOMonitor.class);
	private LinkedList<Double> kbt;
	private LinkedList<Double> tps;
	private LinkedList<Double> mbs;
	
	private LinkedList<Long> user;
	private LinkedList<Long> system;
	private LinkedList<Long> idle;

	public OSXIOMonitor() {
		super();
		setCommandAndArgs("iostat", "5");
		kbt = new LinkedList<Double>();
		tps = new LinkedList<Double>();
		mbs = new LinkedList<Double>();
		user = new LinkedList<Long>();
		system = new LinkedList<Long>();
		idle = new LinkedList<Long>();
	}
	
	// AbstractMonitor
	@Override
	protected void processMonitorOutput(BufferedReader reader) throws IOException {
		reader.readLine();
		reader.readLine();
		reader.readLine();
		while(true) {
			String line = reader.readLine();
			if(line == null) {
				break;
			}
			log.info(line);
			line = line.trim();
			String[] values = line.split("\\s+");
			assert values.length == 9;
			int i = 0;
			kbt.add(Double.parseDouble(values[i++]));
			tps.add(Double.parseDouble(values[i++]));
			mbs.add(Double.parseDouble(values[i++]));
			user.add(Long.parseLong(values[i++]));
			system.add(Long.parseLong(values[i++]));
			idle.add(Long.parseLong(values[i++]));
		}
	}

	@Override
	public void dumpData() {
		System.out.println("Kilobytes per Transfer: "+kbt);
		System.out.println("Transfers per Second:   "+tps);
		System.out.println("Megabytes per Transfer: "+mbs);
		System.out.println("User Mode Time:         "+user);
		System.out.println("System Mode Time:       "+system);
		System.out.println("Idle Time:              "+idle);
	}
	
	// IOMonitor
	@Override
	public double averageKilobytesPerTransfer() {
		return computeAverage(kbt);
	}
	
	@Override
	public List<Double> rawKiloBytesPerTranferMeasurements() {
		return kbt;
	}
	
	@Override
	public double averageTransfersPerSecond() {
		return computeAverage(tps);
	}
	
	@Override
	public List<Double> rawTransfersPerSecond()  {
		return tps;
	}
	
	@Override
	public double averageMegabytesPerSecond() {
		return computeAverage(mbs);
	}
	
	@Override
	public List<Double> rawMegabytesPerSecond()  {
		return mbs;
	}
	
	@Override
	public double averageUserModeTime() {
		return computeAverage(user);
	}
	
	@Override
	public List<Long> rawUserModeTime(){
		return user;
	}
	
	@Override
	public double averageSystemModeTime()  {
		return computeAverage(system);
	}
	
	@Override
	public List<Long> rawSystemModeTime(){
		return system;
	}
	
	@Override
	public double averageIdleModeTime()  {
		return computeAverage(idle);
	}
	
	@Override
	public List<Long> rawIdleModeTime() {
		return idle;
	}
	
	public static void useDisk() throws IOException {
		File file = new File("iomonitor.perf");
		FileOutputStream out = new FileOutputStream(file);
		byte[] bytes = new byte[1024*1024*10];
		for(int j = 0; j < 1024*1024*10; j++) {
			bytes[j] = (byte)(23 * j);
		}
		out.write(bytes);
		out.close();
		assert file.delete();
	}
	
	public static void useCpu() {
		Random r = new Random();
		long first = r.nextLong();
		int secondBound = (int)first;
		if(secondBound < 0) {
			secondBound = -secondBound;
		}
		long second = r.nextInt(secondBound);
		long rem = -1;
		while (rem != 0) {
			rem = first % second;
			first = second;
			second = rem;
		}
	}
	
	public static void main(String[] args) throws Throwable {
		try {
			long starttime = System.currentTimeMillis();
			OSXIOMonitor iom = new OSXIOMonitor();
			System.out.println("Starting..");
			iom.start();
			Thread.sleep(1000);
			if (args.length == 0) {
				while (starttime + 25522 > System.currentTimeMillis()) {
					useDisk();
				}
			} else {
				while (starttime + 25522 > System.currentTimeMillis()) {
					useCpu();
				}
			}
			iom.finish();
			iom.dumpData();
			long duration = iom.getDurationMillis();
			System.out.println("Duration = " + duration);
		} catch (Throwable t) {
			log.error("exception: ", t);
			throw t;
		}
	}	
}


