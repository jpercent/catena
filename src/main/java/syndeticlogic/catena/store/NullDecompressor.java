package syndeticlogic.catena.store;

import java.nio.ByteBuffer;

public class NullDecompressor implements Decompressor {
    private ByteBuffer source;
    private PageDescriptor target;

    public NullDecompressor(PageDescriptor target, ByteBuffer source) {
        this.target = target;
        this.source = source;
    }

    public void run() {
        int iobytes = target.write(source, 0);
        target.setLimit(iobytes);
    }

    public ByteBuffer source() {
        return source;
    }

    @Override
    public Object call() throws Exception {
        run();
        return null;
    }
}
