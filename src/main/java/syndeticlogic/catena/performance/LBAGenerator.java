package syndeticlogic.catena.performance;

public interface LBAGenerator {
    long getNextOffset(int lastIOSize);
}
