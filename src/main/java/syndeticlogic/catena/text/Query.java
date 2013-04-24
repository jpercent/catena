package syndeticlogic.catena.text;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.TreeSet;

import syndeticlogic.catena.text.IdTable.TableType;

public class Query {
    private final InvertedFileReader indexReader;
    private final HashMap<String, InvertedListDescriptor> wordToDescriptor;
    private final HashMap<Integer, String> idToDoc;
    private final String[] terms;
    
    public Query(String prefix, String... terms) throws Exception {
        this.terms = terms;        
        indexReader = new InvertedFileReader();
        indexReader.open(prefix+File.separator+"corpus.index");
        
        DictionaryReader metaReader = new DictionaryReader();
        metaReader.open(prefix+File.separator+"index.meta");
        LinkedList<InvertedListDescriptor> descriptors = new LinkedList<InvertedListDescriptor>();
        idToDoc =  new HashMap<Integer, String>();
        metaReader.loadDictionary(idToDoc, descriptors);
        metaReader.close();
        metaReader = null;
        wordToDescriptor = new HashMap<String, InvertedListDescriptor>();
        System.err.println("Words found in the index "+descriptors.size());
        for(InvertedListDescriptor descriptor : descriptors) {
            
            wordToDescriptor.put(descriptor.getWord(), descriptor);
        }
    }
   
    public TreeSet<String> conjunctiveQuery() throws IOException {
        TreeMap<Integer, InvertedListDescriptor> queryTerms = new TreeMap<Integer, InvertedListDescriptor>();
        TreeSet<String> notFound = new TreeSet<String>() {{
            add("no results found");
        }};
        for(String term : terms) {
            InvertedListDescriptor desc = wordToDescriptor.get(term);
            if(desc == null) {
                System.err.println("Term "+term+" not in dictionary... ");
                return notFound;
            } 
            queryTerms.put(desc.getDocumentFrequency(), desc);
        }
        
        TreeSet<Integer> docIds = new TreeSet<Integer>();
        boolean first = true;
        for(InvertedListDescriptor desc : queryTerms.values()) {
            InvertedList postingList = indexReader.scanEntry(desc);
            if (first) {
                docIds = postingList.getDocumentIds();
                first = false;
            } else {
                docIds = postingList.intersect(docIds);
            }
            if(docIds.size() == 0) {
                System.err.println("Merged out... ");
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
        QueryConfig config = new QueryConfig(args);
        if(!config.parse()) {
            return;
        }
        if(config.getTableType() == TableType.VariableByteCoded) {
            InvertedList.setTableType(TableType.VariableByteCoded);
        }
        InputStreamReader inp = new InputStreamReader(System.in);
        BufferedReader br = new BufferedReader(inp);
        String str = br.readLine();
        Query q = new Query(config.getIndexPrefix(), str.split(" "));
        TreeSet<String> results = q.conjunctiveQuery();
        for(String result : results) {
            System.out.println(result);
        }
    }
    
}
