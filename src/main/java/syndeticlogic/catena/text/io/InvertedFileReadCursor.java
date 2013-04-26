package syndeticlogic.catena.text.io;

import java.util.HashMap;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.io.ReadCursor.BaseReadCursor;
import syndeticlogic.catena.text.postings.InvertedList;

public class InvertedFileReadCursor extends BaseReadCursor {
	private final static Log log = LogFactory.getLog(InvertedFileReadCursor.class);
	private HashMap<Integer, String> idToWord;
    private TreeMap<String, InvertedList> postings;
	
	public InvertedFileReadCursor() {
	    idToWord = new HashMap<Integer, String>();
	    postings = new TreeMap<String, InvertedList>();
	}
	
    @Override
    public void decodeBlock(BlockDescriptor desc) {
        while(desc.offset < desc.buf.length) {
            super.decodeBlock(desc);
        }
    }
	
    @Override
    protected void doDecode(BlockDescriptor desc) {    
        assert desc.buf.length >= desc.offset;
        InvertedList list = InvertedList.create();
        list.decode(desc.buf, desc.offset);
        
        desc.offset += list.size();
        list.setWord(idToWord.get(list.getWordId()));
        assert !postings.containsKey(list.getWord());
        postings.put(list.getWord(), list);
    }	

/*	public int scanBlock(int start, List<InvertedListDescriptor> descriptors, HashMap<Integer, String> idToWord, TreeMap<String, InvertedList> postings) {
	    InvertedListDescriptor cursor = descriptors.get(start);
	    assert buffer.remaining() > cursor.getLength();
        int blockSize = (int) Math.min((long) BLOCK_SIZE, buffer.remaining());
        blockSize = (int)Math.max(blockSize, cursor.getLength());
        byte[] block = new byte[blockSize];

        int sizeCursor = blockSize;
        assert sizeCursor >= cursor.getLength();
        while(true) {
            sizeCursor -= cursor.getLength();
            if(sizeCursor > 0) {
                start++;
                cursor = descriptors.get(start);
            } else {
                break;
            }
        }
        // XXX - if the invertedList for an object is > BLOCK_SIZE we will never read it.
        buffer.get(block, 0, blockSize);
        int offset = decodePostings(block, blockSize, postings, idToWord);
        buffer.position((buffer.position() - (buffer.position() - offset)));
        return start;
	}
	
    public InvertedList scanEntry(InvertedListDescriptor desc) throws IOException {
        buffer = channel.map(MapMode.READ_ONLY, desc.getOffset(), desc.getLength());
        byte[] copy = new byte[desc.getLength()];
        buffer.get(copy);
        InvertedList ret = InvertedList.create();
        ret.decode(copy, 0);
        ret.setWord(desc.getWord());
        return ret;
    }
	*/

}
