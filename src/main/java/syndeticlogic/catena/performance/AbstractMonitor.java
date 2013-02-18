package syndeticlogic.catena.performance;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class AbstractMonitor implements Monitor {
	private Log log = LogFactory.getLog(AbstractMonitor.class);
	private List<String> commandAndArgs;
	private Process process;
	private long start;
	private long finish;
	
	public void start() {
		recordStart();
		ProcessBuilder processBuilder = new ProcessBuilder(commandAndArgs);
		try {
			process = processBuilder.start();
		} catch (IOException e) {
			log.error("IOException recieved trying to run the memory monitory", e);
			throw new RuntimeException(e);
		}
	}
	
	public long getDurationMillis() {
		return finish - start;
	}
	
	public String[] whiteSpaceTokenizer(String line) {
		return line.split("\\s+");
	}
	
	public BufferedReader configureMonitorOutputReader() throws IOException {
		int outputsize = process.getInputStream().available();
		log.debug(outputsize);
		byte[] bytes = new byte[outputsize]; 
		process.getInputStream().read(bytes, 0, outputsize);
		BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(bytes)));
		return reader;
	}
	
	public void finish() {
		try {
		    recordFinish();
			BufferedReader reader = configureMonitorOutputReader();
			processMonitorOutput(reader);
		} catch(IOException e) {
			log.error("IOException reading input stream from vm_stat", e);
			throw new RuntimeException(e);
		} finally {
			process.destroy();
		}
	}
	
	public double computeAverage(List<?> values) {
		if(values.get(0) instanceof Long) {
			long sum = 0;
			for(Object value : values) {
				sum += (Long)value;
			}
			return (double)sum/(double)values.size();
		} else if(values.get(0) instanceof Double) {
			double sum = 0;
			for(Object value : values) {
				sum += (Double)value;
			}
			return sum/(double)values.size();
		} else {
			throw new RuntimeException("unsupported type");
		}
	}
    
    public long getStart() {
        return start;
    }

    public void recordStart() {
        this.start = System.currentTimeMillis();
    }

    public long getFinish() {
        return finish;
    }

    public void recordFinish() {
        this.finish = System.currentTimeMillis();
    }

    public List<String> getCommandAndArgs() {
        return commandAndArgs;
    }
    
    public void setCommandAndArgs(String...commandAndArgs) {
        this.commandAndArgs = new ArrayList<String>(Arrays.asList(commandAndArgs));
    }
}