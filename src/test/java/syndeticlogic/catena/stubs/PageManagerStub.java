package syndeticlogic.catena.stubs;

import java.nio.ByteBuffer;
import java.util.List;

import syndeticlogic.catena.store.Page;
import syndeticlogic.catena.store.PageManager;

public class PageManagerStub extends PageManager {
	public int pageSize = 4096;
	public int unreleasedBuffers = 0;
	public List<Page> pages=null;
	public int pageDesc = 0;
	
	@Override
	public void createPageSequence(String fileName) {
	}

	@Override
	public List<Page> getPageSequence(String fileName) {
		return pages;
	}

	@Override
	public void releasePageSequence(List<Page> pageVector) {
	}

	@Override
	public Page page(String pd) {
		pageDesc++;
		PageDescriptorStub page =  new PageDescriptorStub();
		page.createdByManager = true;
		return page;
	}

	@Override
	public void releasePageDescriptor(Page pagedes) {
	}

	@Override
	public ByteBuffer byteBuffer() {
		unreleasedBuffers++;
		return ByteBuffer.allocateDirect(pageSize);
	}

	@Override
	public void releaseByteBuffer(ByteBuffer buf) {
		buf = null;
		unreleasedBuffers--;
	}

	@Override
	public int pageSize() {
		return pageSize;
	}
	
}