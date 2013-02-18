package syndeticlogic.catena.performance;

import java.util.List;

public interface IOMonitor extends Monitor {
	double getAverageKilobytesPerTransfer();
	List<Double> getRawKiloBytesPerTranferMeasurements();
	double getAverageTransfersPerSecond();
	List<Double> getRawTransfersPerSecond();
	double getAverageMegabytesPerSecond();
	List<Double> getRawMegabytesPerSecond();
	
	double getAverageUserModeTime();
	List<Long> getRawUserModeTime();
	double getAverageSystemModeTime();
	List<Long> getRawSystemModeTime();
	double getAverageIdleModeTime();
	List<Long> getRawIdleModeTime();
}
