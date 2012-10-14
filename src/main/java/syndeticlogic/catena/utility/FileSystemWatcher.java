package syndeticlogic.catena.utility;

import org.apache.commons.vfs2.FileChangeEvent;
import org.apache.commons.vfs2.FileListener;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystemManager;
import org.apache.commons.vfs2.VFS;
import org.apache.commons.vfs2.impl.DefaultFileMonitor;

public abstract class FileSystemWatcher implements FileListener {
    private FileSystemManager fsManager;
    private FileObject listenDir;
    private DefaultFileMonitor fileMonitor;
    private String directory;

    public FileSystemWatcher(String directory) {
        this.directory = directory;
        try {
            start();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    private void start() throws Exception {
        fsManager = VFS.getManager();
        listenDir = fsManager.resolveFile(directory);
        fileMonitor = new DefaultFileMonitor(this);
        fileMonitor.setRecursive(true);
        fileMonitor.addFile(listenDir);
        fileMonitor.start();
    }

    @Override
    public void fileChanged(FileChangeEvent arg0) throws Exception {
    }

    @Override
    public void fileCreated(FileChangeEvent arg0) throws Exception {
    }

    @Override
    public void fileDeleted(FileChangeEvent arg0) throws Exception {
    }

}
