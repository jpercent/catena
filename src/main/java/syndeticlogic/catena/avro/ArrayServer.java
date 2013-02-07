package syndeticlogic.catena.avro;

import java.net.InetSocketAddress;
import java.util.Properties;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import syndeticlogic.catena.array.ArrayRegistry;
import syndeticlogic.catena.array.BinaryArray;
import syndeticlogic.catena.store.PageFactory;
import syndeticlogic.catena.store.PageManager;

public class ArrayServer implements syndeticlogic.catena.avro.CatenaArrayService { 
    public static final Log log = LogFactory.getLog(ArrayServer.class);
    Properties p = new Properties();
    PageFactory pf;
    PageManager pm;
    ArrayRegistry arrayRegistry;
    BinaryArray array;
    int retryLimit = 2;
    Server server;
    
    public ArrayServer(Server server) {
        this.server = server;
    }
    
    public ArrayServer(int port) {
        server = new NettyServer(new SpecificResponder(syndeticlogic.catena.avro.ArrayServer.class, this), new InetSocketAddress(port));
    }


    /*public void setup() throws Exception {
        p.setProperty(PropertiesUtility.CONFIG_BASE_DIRECTORY, prefix);
        p.setProperty(PropertiesUtility.SPLIT_THRESHOLD, "1048676");

        try {
            FileUtils.forceDelete(new File(prefix));
        } catch (Exception e) {
        }

        FileUtils.forceMkdir(new File(prefix));

        Codec.configureCodec(new TypeFactory());
        key = new CompositeKey();
        key.append(prefix);

        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java, 
                PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(null, 4096, 8192);
        SegmentManager.configureSegmentManager(CompressionType.Null, pm);
        arrayRegistry = new ArrayRegistry(p);
        arrayRegistry.createArray(key, Type.INTEGER);
        array = arrayRegistry.createArrayInstance(key);
    }

    public void reconfigure() {
        pf = new PageFactory(PageFactory.BufferPoolMemoryType.Java, 
                PageFactory.CachingPolicy.PinnableLru, PageFactory.PageDescriptorType.Unsynchronized, 
                retryLimit);

        pm = pf.createPageManager(null, 4096, 8192);
        SegmentManager.configureSegmentManager(CompressionType.Null, pm);
        arrayRegistry = new ArrayRegistry(p);
        System.out.println("key =--------------------- "+key.toString());
        array = arrayRegistry.createArrayInstance(key);
    }
    */
    
    @Override
    public Type type(CharSequence id) throws AvroRemoteException, UnknownArrayException {
        
        return null;
    }

    @Override
    public CharSequence createArray(Type t) throws AvroRemoteException,
            InternalServerError {
        return null;
    }


    @Override
    public long beginTranscation(CharSequence arrayId)
            throws AvroRemoteException, UnknownArrayException,
            InternalServerError {
        return 0;
    }


    @Override
    public Void commitTransaction(CharSequence arrayId, long transactionId)
            throws AvroRemoteException, UnknownArrayException,
            InternalServerError {
        return null;
    }


    @Override
    public ScanResult scan(OperationDescriptor operationDescriptor)
            throws AvroRemoteException, InvalidTransactionException,
            UnknownArrayException, InternalServerError {
        return null;
    }


    @Override
    public DeltaResult update(OperationDescriptor operationDescriptor)
            throws AvroRemoteException, InvalidTransactionException,
            UnknownArrayException, InternalServerError {
        return null;
    }


    @Override
    public DeltaResult append(OperationDescriptor operationDescriptor)
            throws AvroRemoteException, InvalidTransactionException,
            UnknownArrayException, InternalServerError {
        return null;
    }


    @Override
    public DeltaResult delete(OperationDescriptor operationDescriptor)
            throws AvroRemoteException, InvalidTransactionException,
            UnknownArrayException, InternalServerError {
        return null;
    }

}
