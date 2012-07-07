package syndeticlogic.catena.predicate;

public interface Predicate {
    boolean satisfies(byte[] raw, int offset, int length);
}
