package syndeticlogic.catena.array;


public interface ValueRecorder {
	int valuesScanned();
	int recordValuesScanned(int remainingBytes);
	int valueScannedSize(int i);
}


