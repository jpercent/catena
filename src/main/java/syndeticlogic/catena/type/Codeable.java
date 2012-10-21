package syndeticlogic.catena.type;

public interface Codeable extends Comparable<Codeable>  {
	String oridinal();
	int size();
	int encode(byte[] dest, int offset);
	int decode(byte[] source, int offset);
	int hashCode();
	boolean equals(Object obj);
	int compareTo(Codeable c);
}
