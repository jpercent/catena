package syndeticlogic.catena.type;

public interface Codeable extends Comparable<Codeable>  {
	int size();
    String oridinal();
	int encode(byte[] dest, int offset);
	int decode(byte[] source, int offset);
	int compareTo(Codeable c);
    boolean equals(Object obj);
    int hashCode();
}
