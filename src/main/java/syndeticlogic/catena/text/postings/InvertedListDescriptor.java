package syndeticlogic.catena.text.postings;

import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.utility.Codec;

public class InvertedListDescriptor implements Codeable {
    private String word;
    private long fileOffset;
    private int length;
    private int documentFrequency;

    public InvertedListDescriptor(String word, long offset, int length, int documentFrequency) {
        this.word = word;
        this.fileOffset = offset;
        this.length = length;
        this.documentFrequency = documentFrequency;
    }
    
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + documentFrequency;
        result = prime * result + length;
        result = prime * result + (int) (fileOffset ^ (fileOffset >>> 32));
        result = prime * result + ((word == null) ? 0 : word.hashCode());
        return result;
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InvertedListDescriptor other = (InvertedListDescriptor) obj;
        if (documentFrequency != other.documentFrequency)
            return false;
        if (length != other.length)
            return false;
        if (fileOffset != other.fileOffset)
            return false;
        if (word == null) {
            if (other.word != null)
                return false;
        } else if (!word.equals(other.word))
            return false;
        return true;
    }

    public String getWord() {
        return word;
    }

    public long getOffset() {
        return fileOffset;
    }

    public int getLength() {
        return length;
    }
    
    public int getDocumentFrequency() {
        return documentFrequency;
    }

    @Override
    public int size() {
        return 2*Type.INTEGER.length()+Type.LONG.length()+Type.STRING.length()+word.length();
    }
   
    @Override
    public String oridinal() {
        throw new RuntimeException("Unsupported");
    }

    @Override
    public int encode(byte[] dest, int offset) {
        int copied = 0;
        copied += Codec.getCodec().encode(word, dest, offset);
        copied += Codec.getCodec().encode(this.fileOffset, dest, offset+copied); 
        copied += Codec.getCodec().encode(length, dest, offset+copied); 
        copied += Codec.getCodec().encode(documentFrequency, dest, offset+copied); 
        return copied;
    }

    @Override
    public int decode(byte[] source, int offset) {
        int decoded = 0;
        word = Codec.getCodec().decodeString(source, offset);
        decoded += Type.STRING.length()+word.length();
        this.fileOffset = Codec.getCodec().decodeLong(source, offset+decoded);
        decoded += Type.LONG.length();
        length = Codec.getCodec().decodeInteger(source, offset+decoded);
        decoded += Type.INTEGER.length();
        documentFrequency = Codec.getCodec().decodeInteger(source, offset+decoded);
        decoded += Type.INTEGER.length();
        return decoded;
    }

    @Override
    public int compareTo(Codeable c) {
        InvertedListDescriptor other = (InvertedListDescriptor)c;
        if(documentFrequency < other.documentFrequency) {
            return -1;
        } else if(documentFrequency == other.documentFrequency) {
            return 0;
        } else {
            return 1;
        }
    }
}