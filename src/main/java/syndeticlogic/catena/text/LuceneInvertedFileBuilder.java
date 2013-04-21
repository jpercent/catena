package syndeticlogic.catena.text;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.IndexWriter;

public class LuceneInvertedFileBuilder { /* implements InvertedFileBuilder  {
    
    /** Creates a new instance of Indexer 
    public Indexer() {
    }
 
    private IndexWriter indexWriter = null;
    
    public IndexWriter getIndexWriter(boolean create) throws IOException {
        if (indexWriter == null) {
            indexWriter = new IndexWriter("index-directory",
                                          new StandardAnalyzer(),
                                          create);
        }
        return indexWriter;
   }    
   
    public void closeIndexWriter() throws IOException {
        if (indexWriter != null) {
            indexWriter.close();
        }
   }
    
    public void indexHotel(Hotel hotel) throws IOException {

        System.out.println("Indexing hotel: " + hotel);
        IndexWriter writer = getIndexWriter(false);
        Document doc = new Document();
        doc.add(new Field("id", hotel.getId(), Field.Store.YES, Field.Index.NO));
        doc.add(new Field("name", hotel.getName(), Field.Store.YES, Field.Index.TOKENIZED));
        doc.add(new Field("city", hotel.getCity(), Field.Store.YES, Field.Index.UN_TOKENIZED));
        doc.add(new Field("description", hotel.getDescription(), Field.Store.YES, Field.Index.TOKENIZED));
        String fullSearchableText = hotel.getName() + " " + hotel.getCity() + " " + hotel.getDescription();
        doc.add(new Field("content", fullSearchableText, Field.Store.NO, Field.Index.TOKENIZED));
        writer.addDocument(doc);
    }   
    
    public void rebuildIndexes() throws IOException {
          //
          // Erase existing index
          //
          getIndexWriter(true);
          //
          // Index all Accommodation entries
          //
          Hotel[] hotels = HotelDatabase.getHotels();
          for(Hotel hotel : hotels) {
              indexHotel(hotel);              
          }
          //
          // Don't forget to close the index writer when done
          //
          closeIndexWriter();
     }

    @Override
    public void addWord(int document, String word) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public int addDocument(String document) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void startBlock(String block) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void completeBlock(String block) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void mergeBlocks() {
        // TODO Auto-generated method stub
        
    }

    @Override
    public HashMap<Integer, String> getIdToDoc() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HashMap<Integer, String> getIdToWord() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TreeMap<String, InvertedList> getPostings() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public LinkedList<InvertedListDescriptor> getInvertedListDescriptors() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public HashMap<String, Integer> getBlockToId() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public String getPrefix() {
        // TODO Auto-generated method stub
        return null;
    }    
}
*/
}
