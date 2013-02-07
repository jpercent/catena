package syndeticlogic.catena.store;

import syndeticlogic.catena.array.ArrayDescriptor.Sizes;
import syndeticlogic.catena.predicate.Predicate;

public class PredicateApplier {
    private final Predicate predicate;
    private final Sizes sizes;
    
    public PredicateApplier(Predicate predicate, Sizes sizes) {
        this.predicate = predicate;
        this.sizes = sizes;
    }

    public int apply(byte[] raw, int offset, int length) {
        int size = sizes.lockAndGetSizeAndIncrement();
        int newLength = length;
        while(length > 0) {
            assert length > 0;
            if(!predicate.satisfies(raw, offset, size)) {
                int newOffset = shiftDown(raw, offset, size);
                length -= size;
                size = sizes.lockAndGetSizeAndIncrement();
                offset = offset + size;
            }
            length -= size; 
        }
        assert length == 0;
        return newLength;
    }
    
    public int shiftDown(byte[] raw, int offset, int length) {
        
        return 0;
    }

    public Predicate getPredicate() {
        return predicate;
    }
    
    public Sizes getSizes() {
        return sizes;
    }
}
/*
-
|
|
|
-
|
|
-
|
|
|
|
-
*/