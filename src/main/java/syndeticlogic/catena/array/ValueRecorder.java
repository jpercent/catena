package syndeticlogic.catena.array;


public interface ValueRecorder {
	int valuesScanned();
	int recordValuesScanned(SegmentCursor cursor);
	int valueScannedSize(int i);
}


