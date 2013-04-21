package syndeticlogic.catena.text;

import java.io.Serializable;

@SuppressWarnings("serial")
public class InvertedListDescriptor implements Serializable {
    private final String word;
    private final long offset;
    private final int length;
    private final int documentFrequency;

    public InvertedListDescriptor(String word, long offset, int length, int documentFrequency) {
        this.word = word;
        this.offset = offset;
        this.length = length;
        this.documentFrequency = documentFrequency;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + documentFrequency;
        result = prime * result + length;
        result = prime * result + (int) (offset ^ (offset >>> 32));
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
        if (offset != other.offset)
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
        return offset;
    }

    public int getLength() {
        return length;
    }
    
    public int getDocumentFrequency() {
        return documentFrequency;
    }
}