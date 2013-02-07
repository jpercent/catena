package syndeticlogic.catena.store;

import syndeticlogic.catena.predicate.Predicate;

public class ObjectUpdate {
    private Predicate predicate;
    private PageManager pageManager;
    private String fileName;
    
    public ObjectUpdate(Predicate predicate, PageManager pageManager, String fileName) {
        this.predicate = predicate;
        this.pageManager = pageManager;
        this.fileName = fileName;
    }
    
    public void update(byte[] buffer, int bufferOffset, long fileOffset, int oldLen, int newLen) {
        PageUpdate pageIO = new PageUpdate(pageManager, fileName);
        pageIO.update(buffer, bufferOffset, oldLen, newLen, fileOffset);
    }
}
