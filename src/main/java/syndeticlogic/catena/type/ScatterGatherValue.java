package syndeticlogic.catena.type;

import java.util.LinkedList;

public abstract class ScatterGatherValue extends Value {
    protected final LinkedList<PartialValue> values;
    private boolean cache;
    
    public ScatterGatherValue() {
        super(null, 0, 0);
        values = new LinkedList<PartialValue>();
        length = 0;
        cache = false;
    }
    
    public void add(byte[] data, int offset, int length) {
        values.add(new PartialValue(data, offset, length));
        this.length += length;
    }
    
    public byte[] gather() {
        byte[] retdata = new byte[length];
        int retoffset = 0;
        for(PartialValue value : values) {
            System.arraycopy(value.data, value.offset, retdata, retoffset, value.length);
            retoffset += value.length;
        }
       return retdata;
    }
    
    public void cacheGather(boolean cache) {
        this.cache = true;
    }
    
    public boolean cacheGather() {
        return this.cache;
    }
    
    @Override
    public void reset(byte[] data, int offset, int length) {
        values.clear();
        length = 0;
        add(data, offset, length);
    }
    
    @Override
    public byte[] data() {
        return gather();
    }
    
    @Override
    public int offset() {
        return 0;
    }
    
    @Override
    public int length() {
        return length;
    }

    protected class PartialValue {
        public PartialValue(byte[] d, int o, int l) {
            this.data = d;
            this.offset = o;
            this.length = l;
        }
        public byte[] data;
        public int offset;
        public int length;
    }
}
