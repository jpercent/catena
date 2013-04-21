package syndeticlogic.catena.text;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;

public class Query {
    private final InvertedFileReader fileReader;
    private final HashMap<String, InvertedListDescriptor> wordToDescriptor;
    private final HashMap<Integer, String> idToDoc;
    //private final String prefix;
    private final String[] terms;
    
    @SuppressWarnings("unchecked")
    public Query(String prefix, String... terms) throws Exception {
        this.terms = terms;        
        fileReader = new InvertedFileReader();
        fileReader.open(prefix+".index");
        
        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(prefix+File.separator+".meta"));
        List<InvertedListDescriptor> descriptors = (List<InvertedListDescriptor>) objectInputStream.readObject();
        wordToDescriptor = new HashMap<String, InvertedListDescriptor>();
        for(InvertedListDescriptor descriptor : descriptors) {
            wordToDescriptor.put(descriptor.getWord(), descriptor);
        }
        idToDoc = (HashMap<Integer, String>)objectInputStream.readObject();
        objectInputStream.close();
    }
   
    public TreeSet<String> conjunctiveQuery() throws IOException {
        TreeMap<Integer, InvertedListDescriptor> queryTerms = new TreeMap<Integer, InvertedListDescriptor>();
        TreeSet<String> notFound = new TreeSet<String>() {{
            add("no results found");
        }};
        for(String term : terms) {
            System.out.println("Term = "+term);
            InvertedListDescriptor desc = wordToDescriptor.get(term);
            if(desc == null) {
                return notFound;
            } 
            System.out.println("desc = "+desc.getOffset()+" frequence "+desc.getDocumentFrequency());
            queryTerms.put(desc.getDocumentFrequency(), desc);
        }
        
        TreeSet<Integer> docIds = new TreeSet<Integer>();
        boolean first = true;
        for(InvertedListDescriptor desc : queryTerms.values()) {
            InvertedList postingList = fileReader.scanEntry(desc);
            System.out.println("Searching "+postingList.getWord());
            if (first) {
                docIds = postingList.getDocumentIds();
                first = false;
            } else {
                docIds = postingList.intersect(docIds);
            }
            System.out.println("docids = "+docIds);
            if(docIds.size() == 0) {
                return notFound;
            }
        }
        
        TreeSet<String> results = new TreeSet<String>();
        for(Integer docId : docIds) {
            String document = idToDoc.get(docId);
            assert document != null;
            results.add(document);
        }
        return results;
    }
    
    public static void main(String[] args) throws Exception {
        System.out.println(System.getProperty("user.dir"));
        //Query q = new Query("target"+File.separator+"corpus-manager-block-merger-test"+File.separator+"merged/intermediate-merge-target.0", "&", "is");
        //Query q = new Query("target"+File.separator+"corpus-manager-block-merger-test"+File.separator+"single/", "&", "is");
        Query q = new Query("target"+File.separator+"corpus-manager-block-merger-test"+File.separator+"merged/", "campaign", "for");
        TreeSet<String> results = q.conjunctiveQuery();
        for(String result : results) {
            System.out.println(result);
        }
    }
    
}
