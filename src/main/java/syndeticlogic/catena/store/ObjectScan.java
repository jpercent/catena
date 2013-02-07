package syndeticlogic.catena.store;

public class ObjectScan {
    private PredicateApplier predicate;
    private PageManager pageManager;
    private String fileName;
    
    public ObjectScan(PredicateApplier predicate, PageManager pageManager, String fileName) {
        this.predicate = predicate;
        this.pageManager = pageManager;
        this.fileName = fileName;
    }
    
    public int scan(byte[] buffer, int bufferOffset, long fileOffset, int length) {
        if(this.predicate == null) {
            return unfilteredScan(buffer, bufferOffset, fileOffset, length);
        } else {
            return filteredScan(buffer, bufferOffset, fileOffset, length);
        }
    }
    
    public int unfilteredScan(byte[] buffer, int bufferOffset, long fileOffset, int length) {
        PageScan pageIO = new PageScan(pageManager, fileName);
        return pageIO.scan(buffer, bufferOffset, length, fileOffset);
    }
    
    public int filteredScan(byte[] buffer, int bufferOffset, long fileOffset, int length) {
        return unfilteredScan(buffer, bufferOffset, fileOffset, length);
        /* filtered scans of large objects take a lot of memory, the upper layers should 
         * be prepared for that 
         */
    }
}
