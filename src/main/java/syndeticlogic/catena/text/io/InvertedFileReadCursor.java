package syndeticlogic.catena.text.io;

import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.text.io.ReadCursor.BaseReadCursor;
import syndeticlogic.catena.text.postings.InvertedList;

public class InvertedFileReadCursor extends BaseReadCursor {
	private final static Log log = LogFactory.getLog(InvertedFileReadCursor.class);
	private Map<Integer, String> idToWord;
    private TreeMap<String, InvertedList> postings;
	
	public InvertedFileReadCursor(Map<Integer, String> idToWord) {
	    this.idToWord = idToWord;
	    postings = new TreeMap<String, InvertedList>();
	}

    public TreeMap<String, InvertedList> getInvertedList() {
        return postings;
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
        //System.out.print("do decod ");BlockReader.printBinary(desc.buf, desc.offset, 10);
        list.decode(desc.buf, desc.offset);
        desc.offset += list.size();
        list.setWord(idToWord.get(list.getWordId()));
        //System.out.println("Postings = "+postings);
        //System.out.println("key == "+list.getWord());
        if(postings.containsKey(list.getWord())) {
            throw new RuntimeException("Duplicate key = "+list.getWord());
            //log.warn("Key --->"+list.getWord()+"<-- duplicate ");
        }
        postings.put(list.getWord(), list);
    }
}
