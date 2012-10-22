package syndeticlogic.catena.type;

public class GenericValue<T extends Value> {
    Class<T> clazz;

    public static <T extends Value> GenericValue<T> create(Class<T> clazz) {
        return new GenericValue<T>(clazz);
    }

    public GenericValue(Class<T> clazz) {
        this.clazz = clazz;
    }

    public T createInstance() throws InstantiationException, IllegalAccessException {
        return clazz.newInstance();
    }
}
