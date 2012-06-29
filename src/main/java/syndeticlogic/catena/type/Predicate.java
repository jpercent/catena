package syndeticlogic.catena.type;

public interface Predicate {
    boolean satisfies(byte[] raw, int offset, int length);
}
