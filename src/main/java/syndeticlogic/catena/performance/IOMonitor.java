package syndeticlogic.catena.performance;

import java.util.List;

public interface IOMonitor {
	double averageKilobytesPerTransfer();
	List<Double> rawKiloBytesPerTranferMeasurements();
	double averageTransfersPerSecond();
	List<Double> rawTransfersPerSecond();
	double averageMegabytesPerSecond();
	List<Double> rawMegabytesPerSecond();
	
	double averageUserModeTime();
	List<Long> rawUserModeTime();
	double averageSystemModeTime();
	List<Long> rawSystemModeTime();
	double averageIdleModeTime();
	List<Long> rawIdleModeTime();
}
