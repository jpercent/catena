package syndeticlogic.catena.performance;

import java.util.Random;

public class RandomSequentialLBAGenerator extends SequentialLBAGenerator {
    protected Random random;
    protected int randomBound;
    
    public RandomSequentialLBAGenerator(int blockSize, int randomBound) {
        super(blockSize, 0);
        this.randomBound = randomBound;
        this.random = new Random();
    }
    
    public long getNextOffset(int lastIOSize) {
        this.lastIOSize = lastIOSize;
        skipSize = random.nextInt(randomBound);
        assert skipSize >= 0;
        updateCursor();
        return cursor;
    }
}
