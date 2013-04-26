package syndeticlogic.catena.text.io;

public interface ReadCursor {
    int headerSize();
    void decodeHeader(BlockDescriptor blockDesc);
    void decodeBlock(BlockDescriptor blockDesc);

    abstract class BaseReadCursor implements ReadCursor {
        protected byte[] leftover=null;
        
        protected abstract void doDecode(BlockDescriptor blockDesc);
        
        @Override
        public int headerSize() { return 0; }

        @Override
        public void decodeHeader(BlockDescriptor blockDesc) { return; }
        
        @Override
        public void decodeBlock(BlockDescriptor blockDesc) {
            mixinLeftover(blockDesc);
            Throwable t = null;;
            try {
                doDecode(blockDesc);                
            } catch(AssertionError e){
                t = e;
            } catch(IndexOutOfBoundsException e) {
                t = e;
            }
            
            if(t != null) {
                addLeftover(blockDesc);
            }
        }
        
        protected void mixinLeftover(BlockDescriptor blockDesc) {
            if(leftover != null) {
                byte[] newBlock = new byte[leftover.length + blockDesc.buf.length];
                System.arraycopy(leftover, 0, newBlock, 0, leftover.length);
                System.arraycopy(blockDesc.buf, blockDesc.offset, newBlock, leftover.length, blockDesc.buf.length);
                leftover = null;
                blockDesc.buf = newBlock;
                blockDesc.offset = 0;
            }
        }
        
        protected void addLeftover(BlockDescriptor blockDesc) {
            assert blockDesc.buf.length >= blockDesc.offset;
            int remaining = blockDesc.buf.length - blockDesc.offset;
            assert leftover == null;
            leftover = new byte[remaining];
            System.arraycopy(blockDesc.buf, blockDesc.offset, leftover, 0, remaining);
            blockDesc.offset = blockDesc.buf.length;
        }
    }
    
    public static class BlockDescriptor {
        byte[] buf;
        int offset;
    }
}
