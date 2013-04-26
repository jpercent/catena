package syndeticlogic.catena.text.postings;

import java.util.Iterator;
import java.util.TreeSet;

import syndeticlogic.catena.text.postings.IdTable.TableType;
import syndeticlogic.catena.type.Codeable;
import syndeticlogic.catena.type.Type;
import syndeticlogic.catena.utility.Codec;

public class InvertedList implements Codeable {
    private static IdTable.TableType tableType=TableType.UncodedArray;
    private int wordId;
    private String word;
    private int documentFrequency;
    private IdTable table;
    
    public InvertedList(IdTable table) {
        this.wordId = -1;
        this.documentFrequency = 0;
        this.table = table;
        word = null;
    }
    
    public InvertedList(int wordId, IdTable table) {
        this.wordId = wordId;
        this.documentFrequency = 0;
        this.table = table;
        word = null;
    }
    
    public void addDocumentId(int docId) {
        documentFrequency++;
        table.addId(docId);
    }

    public void merge(InvertedList list) {        
        assert getLastDocId() < list.getLastDocId();
        list.resetIterator();
        while(list.hasNext()) {
            addDocumentId(list.advanceIterator());
        }
    }
    
    public int getLastDocId() {
        return table.getLastId();
    }

    public int getFirstDocId() {
        return table.getFirstId();
    }

    public TreeSet<Integer> getDocumentIds() {
        TreeSet<Integer> docIds = new TreeSet<Integer>();
        table.resetIterator();
        while(table.hasNext()) {
            docIds.add(table.advanceIterator());
        }
        return docIds;
    }

    public TreeSet<Integer> intersect(TreeSet<Integer> docIds) {
        table.resetIterator();
        Iterator<Integer> iter = docIds.iterator();
        if(!table.hasNext() || !iter.hasNext()) {
            return new TreeSet<Integer>();
        }
        
        int nextPosting = table.advanceIterator();
        int nextDocId = iter.next();        
        TreeSet<Integer> matches = new TreeSet<Integer>();
        while(true) {
            if (nextPosting > nextDocId) {
                if(iter.hasNext()) {
                    nextDocId = iter.next();
                } else { 
                    break;
                }
            } else if (nextPosting < nextDocId) {
                if(table.hasNext()) {
                    nextPosting = table.advanceIterator();
                } else { 
                    break;
                }
            } else {
                matches.add(nextDocId);
                if (table.hasNext() && iter.hasNext()) {
                    nextPosting = table.advanceIterator();
                    nextDocId = iter.next();
                } else {
                    break;
                }
            }
        }
        return matches;
    }
    
    @Override
    public int encode(byte[] dest, int offset) {
        int copied = 0;
        Codec.getCodec().encode(wordId, dest, offset+copied);
        copied += Type.INTEGER.length();
        Codec.getCodec().encode(documentFrequency, dest, offset+copied);
        copied += Type.INTEGER.length();
        copied += table.encode(dest, offset+copied);
        return copied;
    }
    
    @Override
    public int decode(byte[] source, int offset) {
        int copied = 0;
        wordId = Codec.getCodec().decodeInteger(source, offset+copied);
        copied += Type.INTEGER.length();

        documentFrequency = Codec.getCodec().decodeInteger(source, offset+copied);
        copied += Type.INTEGER.length();
        
        copied += table.decode(source, offset+copied);
        
        return copied;
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
        int size = 2*Type.INTEGER.length();
        size += table.size();
        return size;
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
        if (table == null) {
            if (other.table != null)
                return false;
        } else if (!table.equals(other.table))
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
        table.resetIterator();
    }
    
    public int advanceIterator() {
        return table.advanceIterator();
    }
    
    public boolean hasNext() {
        return table.hasNext();
    }
    
    public void dumpDocs() {
        table.dumpDocs();
    }
    
    public int getDocumentFrequency() {
        return documentFrequency;
    }
    
    public int getWordId() {
        return wordId;
    }
    
    public void setWordId(int wordId) {
        this.wordId = wordId;
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
        return IdTable.getPageSize();
    }
    
    public static void setPageSize(int pageSize) {
        IdTable.setPageSize(pageSize);
    }

    public static void setTableType(IdTable.TableType type) {
        tableType = type;
    }

    public static TableType getTableType() {
        return tableType;
    }
    
    public static InvertedList create() {
        return create(-1);
    }
    
    public static InvertedList create(Integer wordId) {
        IdTable idTable=null;
        switch(tableType) {
        case UncodedTable:
            idTable = new UncodedIdTable();
            break;
        case UncodedArray:
            idTable = new UncodedIdArray();
            break;
        case VariableByteCodedTable:
            idTable = new VariableByteCodedIdTable();
            break;
        case VariableByteCodedArray:
            idTable = new VariableByteCodedIdArray();
            break;
            
        default:
            assert false;
        }
        return new InvertedList(wordId, idTable);
    }
}
