package syndeticlogic.catena.text;

public class InvertedListDescriptor {
    private final int wordId;
    private final long offset;
    private final int length;

    public InvertedListDescriptor(int wordId, long offset, int length) {
        this.wordId = wordId;
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + length;
        result = prime * result + (int) (offset ^ (offset >>> 32));
        result = prime * result + wordId;
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
        if (length != other.length)
            return false;
        if (offset != other.offset)
            return false;
        if (wordId != other.wordId)
            return false;
        return true;
    }

    public int getWordId() {
        return wordId;
    }

    public long getOffset() {
        return offset;
    }

    public int getLength() {
        return length;
    }
}