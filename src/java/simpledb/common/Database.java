package simpledb.common;

import simpledb.storage.BufferPool;
import simpledb.storage.LogFile;
import simpledb.transaction.LockManager;

import java.io.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Database is a class that initializes several static variables used by the
 * database system (the catalog, the buffer pool, and the log files, in
 * particular.)
 * <p>
 * Provides a set of methods that can be used to access these variables from
 * anywhere.
 * 
 * @Threadsafe
 */
public class Database {
    private static final AtomicReference<Database> _instance = new AtomicReference<>(new Database());
    private final Catalog _catalog;
    private final BufferPool _bufferpool;
    private final LockManager _lockManager;

    private final static String LOGFILENAME = "log";
    private final LogFile _logfile;

    private Database() {
        _catalog = new Catalog();
        _bufferpool = new BufferPool(BufferPool.DEFAULT_PAGES);
        _lockManager = new LockManager();
        LogFile tmp = null;
        try {
            tmp = new LogFile(new File(LOGFILENAME));
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(1);
        }
        _logfile = tmp;
        // startControllerThread();
    }

    /** Return the log file of the static Database instance */
    public static LogFile getLogFile() {
        return _instance.get()._logfile;
    }

    /** Return the buffer pool of the static Database instance */
    public static BufferPool getBufferPool() {
        return _instance.get()._bufferpool;
    }

    public static LockManager getLockManager() {
        return _instance.get()._lockManager;
    }

    /** Return the catalog of the static Database instance */
    public static Catalog getCatalog() {
        return _instance.get()._catalog;
    }

    /**
     * Method used for testing -- create a new instance of the buffer pool and
     * return it
     */
    public static BufferPool resetBufferPool(int pages) {
        java.lang.reflect.Field bufferPoolF=null;
        java.lang.reflect.Field lockManagerF=null;
        try {
            bufferPoolF = Database.class.getDeclaredField("_bufferpool");
            bufferPoolF.setAccessible(true);
            bufferPoolF.set(_instance.get(), new BufferPool(pages));

            lockManagerF = Database.class.getDeclaredField("_lockManager");
            lockManagerF.setAccessible(true);
            lockManagerF.set(_instance.get(), new LockManager());
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }
//        _instance._bufferpool = new BufferPool(pages);
        return _instance.get()._bufferpool;
    }

    // reset the database, used for unit tests only.
    public static void reset() {
        _instance.set(new Database());
    }

}
