package syndeticlogic.catena.text;

public interface WriteCursor {
    int reserveHeaderLength();
    void setFileOffset(long fileOffset);
    boolean hasNext();
    int nextLength();
    int encodeNext(byte[] buffer, int offset);
    int encodeHeader(byte[] buffer, int offset);
    
    public abstract class BaseWriteCursor implements WriteCursor {
        protected int headerSize;
        protected long fileOffset;
        protected long bytesWritten;
        public int reserveHeaderLength() { return headerSize; }
        public void setFileOffset(long fileOffset) { this.fileOffset = fileOffset; }
        public int encodeHeader(byte[] buffer, int offset) { return 0; }
    }
}
