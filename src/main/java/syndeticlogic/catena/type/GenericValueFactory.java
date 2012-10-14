package syndeticlogic.catena.type;

public class GenericValueFactory<T extends Value> {
    Class<T> clazz;

    public static <T extends Value> GenericValueFactory<T> create(Class<T> clazz) {
        return new GenericValueFactory<T>(clazz);
    }

    public GenericValueFactory(Class<T> clazz) {
        this.clazz = clazz;
    }

    public T createInstance() throws InstantiationException, IllegalAccessException {
        return clazz.newInstance();
    }
}
