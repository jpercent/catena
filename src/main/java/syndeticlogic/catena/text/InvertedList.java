package syndeticlogic.catena.text;

import java.nio.ByteBuffer;
import java.util.Arrays;

import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class InvertedList implements Codeable {
    private static int PAGE_SIZE = 8192;
    private int wordId;
    private String word;
    private int documentFrequency;
    private int[][] documentIds;
    private int slots;
    private int slotCursor;
    private int pageCursor = 0;
    private int slotIterator = 0;
    private int pageIterator = 0;
    
    public InvertedList() {
        this.wordId = -1;
        this.documentFrequency = 0;
        documentIds = new int[PAGE_SIZE][];
        documentIds[0] = new int[PAGE_SIZE];
        slots = PAGE_SIZE;
        slotCursor = 0;
        pageCursor = 0;
        word = null;
    }
    
    public InvertedList(int wordId) {
        this.wordId = wordId;
        this.documentFrequency = 0;
        documentIds = new int[PAGE_SIZE][];
        documentIds[0] = new int[PAGE_SIZE];
        slots = PAGE_SIZE;
        slotCursor = 0;
        pageCursor = 0;
        word = null;
    }
    
    public void addDocumentId(int docId) {
        if(pageCursor == PAGE_SIZE) {
            addPage();
        }
        documentIds[slotCursor][pageCursor] = docId;
        documentFrequency++;
        pageCursor++;
    }

    private void addPage() {
        if(slotCursor + 1 == slots) {
            expand();
        }
        slotCursor++;
        pageCursor = 0;
        documentIds[slotCursor] = new int[PAGE_SIZE];
    }

    private void expand() {
        int[][] newDocs = new int[slots+PAGE_SIZE][];
        System.arraycopy(documentIds, 0, newDocs, 0, slots);
        slots += PAGE_SIZE;
        documentIds = newDocs;
    }
            
    @Override
    public int encode(byte[] dest, int offset) {
        int copied = 0;
        Codec.getCodec().encode(wordId, dest, offset+copied);
        copied += Type.INTEGER.length();
        
        Codec.getCodec().encode(documentFrequency, dest, offset+copied);
        copied += Type.INTEGER.length();
       
        int docIdsSize = documentIdsSize();
        Codec.getCodec().encode(docIdsSize, dest, offset+copied);
        copied += Type.INTEGER.length();
     
        int computedDocSize = flattenTable(dest, offset+copied);
        assert computedDocSize == docIdsSize;
        copied += computedDocSize;
        return copied;
    }
    
    private int flattenTable(byte[] dest, int offset) {
        int copied = 0;
        for(int i = 0; i <= slotCursor; i++) {
            int length = PAGE_SIZE;
            if(i == slotCursor) {
                length = pageCursor;
            }
            /*System.out.println("offset "+offset);
            System.out.println("Lenght "+length);
            System.out.println("dest length "+dest.length); */
            ByteBuffer transferBuff = ByteBuffer.wrap(dest, offset+copied, length*Type.INTEGER.length());
            transferBuff.asIntBuffer().put(documentIds[i], 0, length);
            copied += length*Type.INTEGER.length();
        }
        return copied;
    }
    
    @Override
    public int decode(byte[] source, int offset) {
        int copied = 0;
        wordId = Codec.getCodec().decodeInteger(source, offset+copied);
        copied += Type.INTEGER.length();
        
        documentFrequency = Codec.getCodec().decodeInteger(source, offset+copied);
        copied += Type.INTEGER.length();
        
        int docSize = Codec.getCodec().decodeInteger(source, offset+copied);
        copied += Type.INTEGER.length();
        int tableSize = constructTable(source, offset+copied, docSize);
        assert docSize == tableSize;
        return copied+tableSize;
    }

    private int constructTable(byte[] source, int offset, int remaining) {
        int accumulation = 0;
        while (remaining > 0) {
            int blockSize = PAGE_SIZE;
            if(remaining/Type.INTEGER.length() < blockSize) {
                blockSize = remaining/Type.INTEGER.length();
            }
            pageCursor = blockSize;
            ByteBuffer transferBuf = ByteBuffer.wrap(source, offset+accumulation, remaining); 
            transferBuf.asIntBuffer().get(documentIds[slotCursor], 0, blockSize);
            if(pageCursor == PAGE_SIZE) {
                addPage();
            }
            remaining -= blockSize*Type.INTEGER.length();
            accumulation += blockSize*Type.INTEGER.length();
        }
        assert remaining == 0;
        return accumulation;
    }
    
    @Override
    public int compareTo(Codeable c) {
        InvertedList o = (InvertedList) c;
        if (word == null || "".equals(word) || o.word == null || "".equals(word) ) {
            throw new RuntimeException("compareTo requires that word values have been set");
        }
        int compareResult = word.compareTo(o.word);
        return compareResult;
    }
    
    @Override
    public int size() {
        int size = 3*Type.INTEGER.length();
        size += documentIdsSize();
        return size;
    }
    
    private int documentIdsSize() {
        return (/* filled up pages + last, partial page */ Type.INTEGER.length() * slotCursor * PAGE_SIZE) 
        + (Type.INTEGER.length() * pageCursor);
    }
    @Override
    public int hashCode() {
    	final int prime = 31;
    	int result = 1;
    	result = prime * result + documentFrequency;
    	result = prime * result + wordId;
    	return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        InvertedList other = (InvertedList) obj;
        if (documentFrequency != other.documentFrequency)
            return false;
        if (wordId != other.wordId)
            return false;
        return true;
    }
    
	public boolean deepCompare(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		InvertedList other = (InvertedList) obj;
		if (documentFrequency != other.documentFrequency)
			return false;
		if (!Arrays.deepEquals(documentIds, other.documentIds))
			return false;
		if (pageCursor != other.pageCursor)
			return false;
		if (pageIterator != other.pageIterator)
			return false;
		if (slotCursor != other.slotCursor)
			return false;
		if (slotIterator != other.slotIterator)
			return false;
		if (slots != other.slots)
			return false;
		if (word == null) {
			if (other.word != null)
				return false;
		} else if (!word.equals(other.word))
			return false;
		if (wordId != other.wordId)
			return false;
		return true;
	}


    
    public void resetIterator() {
        slotIterator = 0;
        pageIterator = 0;
    }

    public boolean hasNext() {
        if(slotIterator <= slotCursor) {
            return true;
        } else if(pageIterator < pageCursor) {
            return true;
        }
        return false;
    }
    
    public int advanceIterator() {
        int ret = documentIds[slotIterator][pageIterator++];
        if(pageIterator == PAGE_SIZE) {
            slotIterator++;
            pageIterator=0;         
        }
        return ret;
    }
    
    public int getWordId() {
        return wordId;
    }
    
    public void setWord(String word) {
        this.word = word;
    }
    
    public String getWord() {
        return word;
    }
    
    @Override
    public String oridinal() {
        throw new RuntimeException("ordinal is unsupported");
    }
    
    public static int getPageSize() {
        return PAGE_SIZE;
    }
    
    public static void setPageSize(int pageSize) {
        InvertedList.PAGE_SIZE = pageSize;
    }
}
