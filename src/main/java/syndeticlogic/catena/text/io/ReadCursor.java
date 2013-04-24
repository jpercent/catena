package syndeticlogic.catena.text.io;

public interface ReadCursor {
    int headerSize();
    int decodeHeader(byte[] block, int blockSize);
    int decodeBlock(byte[] block, int blockSize);

    abstract class BaseReadCursor implements ReadCursor {
        protected byte[] leftover;

        @Override
        public int headerSize() {
            return 0;
        }

        @Override
        public int decodeHeader(byte[] block, int blockSize) {
            return 0;
        }

        void addLeftover(byte[] block, int blockOffset) {
            int offset = 0;
            int remaining = block.length - blockOffset;
            if(leftover != null) {
                int newSize = leftover.length + remaining;
                byte[] newleftover = new byte[newSize];
                System.arraycopy(leftover, leftover.length, newleftover, 0, leftover.length);
                offset = newSize;
            }
            System.arraycopy(block, blockOffset, leftover, offset, block.length);
        }
    }
}
