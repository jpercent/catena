package syndeticlogic.catena.avro;

public class ArrayFactory {
    public void createArrayType() {
      Class a = this.getClass();
      System.out.println("a = "+ a.getCanonicalName());
    }
    
    public static void main(String[] args) {
        ArrayFactory array = new ArrayFactory();
        array.createArrayType();
    }
}
