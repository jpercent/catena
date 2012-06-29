package syndeticlogic.catena.array;

public interface ElementTable {
    ElementDescriptor find(long index);
    int update(long index, int size);
    void append(int size);
    int delete(long index);
    void persist();
}
