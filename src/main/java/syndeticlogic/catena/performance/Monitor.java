package syndeticlogic.catena.performance;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;

public interface Monitor {	
	void start();
	void finish();
	void processMonitorOutput(BufferedReader reader) throws IOException;
	void dumpData();
	long getStart();
	void recordStart();
	long getFinish();
	void recordFinish();
	long getDurationMillis();
	List<String> getCommandAndArgs();
	void setCommandAndArgs(String...comandAndArgs);
}