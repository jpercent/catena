package syndeticlogic.catena.stubs;

import java.nio.ByteBuffer;
import java.util.List;

import syndeticlogic.catena.store.PageDescriptor;
import syndeticlogic.catena.store.PageManager;

public class PageManagerStub extends PageManager {
	public int pageSize;
	public int unreleasedBuffers = 0;
	public List<PageDescriptor> pages=null;
	public int pageDesc = 0;

	@Override
	public void createPageSequence(String fileName) {
	}

	@Override
	public List<PageDescriptor> getPageSequence(String fileName) {
		return pages;
	}

	@Override
	public void releasePageSequence(List<PageDescriptor> pageVector) {
	}

	@Override
	public PageDescriptor pageDescriptor(String pd) {
		pageDesc++;
		PageDescriptorStub page =  new PageDescriptorStub();
		page.createdByManager = true;
		return page;
	}

	@Override
	public void releasePageDescriptor(PageDescriptor pagedes) {
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