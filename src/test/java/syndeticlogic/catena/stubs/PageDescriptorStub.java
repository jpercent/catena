package syndeticlogic.catena.stubs;

import syndeticlogic.catena.store.Page;

public class PageDescriptorStub extends Page {
	
	public int limit;
	public byte[] buffer;
	public int boffset;
	public int poffset;
	public int lastSize;
	public int pinCount;
	public int size=4096;
	public String id;
	public boolean createdByManager;
	
	public PageDescriptorStub() {
		super();
	}
	
	@Override
	public int read(byte[] dest, int destOffset, int pageOffset, int size) {
		buffer = dest;
		boffset = destOffset;
		poffset = pageOffset;
		lastSize = size;
		return size;
	}	
	@Override
	public int write(byte[] source, int sourceOffset, int pageOffset, int size) {
		buffer = source;
		boffset = sourceOffset;
		poffset = pageOffset;
		this.lastSize = size;
		return size;
	}
	@Override
	public void pin() {
		assert buffer != null;
		pinCount++;
	}
	@Override	
	public void unpin() {
		assert pinCount > 0 && buffer != null;
		pinCount--;
	}
	
	@Override
	public int size() {
		return size;
	}
	
	@Override
	public void setLimit(int limit) {
		this.limit = limit;
	}
	
	@Override
	public int limit() {
		return  limit;
	}
}
