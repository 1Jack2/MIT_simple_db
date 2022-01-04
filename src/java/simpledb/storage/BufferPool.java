package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import javax.xml.crypto.Data;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final Page[] pages;

    /* LRU cache of pages. */
    /** map pageId to page slotId */
    private final Map<PageId, Integer> pageTable;
    private final List<PageId> lruPids;
    private final Set<Integer> availableSlots;
    private final Object metaLatch = new Object();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pages = new HeapPage[numPages];
        pageTable = new HashMap<>();
        lruPids = new LinkedList<>();
        availableSlots = new HashSet<>();
        for (int i = 0; i < numPages; i++) {
            availableSlots.add(i);
        }
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        if (tid != null) {
            switch (perm) {
                case READ_ONLY:
                    Database.getLockManager().lockShared(tid, pid);
                    break;
                case READ_WRITE:
                    Database.getLockManager().lockExclusive(tid, pid);
                    break;
                default:
                    throw new DbException("Unknown permission: " + perm);
            }
        }
        synchronized (metaLatch) {
            if (pageTable.containsKey(pid)) {
                // Move to head
                lruPids.remove(pid);
                lruPids.add(pid);

                return pages[pid2slotId(pid)];
            }
            Integer next = getSlotIdAndUpdatePageTable(pid);
            return pages[next] = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
        }
    }

    private Integer getSlotIdAndUpdatePageTable(PageId pid) throws DbException {
        Integer next = getAvailableSlotId();
        availableSlots.remove(next);
        lruPids.add(pid);
        pageTable.put(pid, next);
        return next;
    }

    /**
     * Get an available slot id
     * @return slotId
     */
    private Integer getAvailableSlotId() throws DbException {
        Iterator<Integer> iterator = availableSlots.iterator();
        if (!iterator.hasNext()) {
            evictPage();
            return getAvailableSlotId();
        }
        return iterator.next();
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        Database.getLockManager().unlock(tid, pid);
        synchronized (metaLatch) {
            availableSlots.add(pid2slotId(pid));
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        if (tid == null) return false;
        return Database.getLockManager().holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            restorePages(tid);
        }

        // release all locks
        for (PageId pageId : pageTable.keySet()) {
            if (holdsLock(tid, pageId)) {
                Database.getLockManager().unlock(tid, pageId);
            }
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> mPages = dbFile.insertTuple(tid, t);
        synchronized (metaLatch) {
            for (Page page : mPages) {
                Integer slotId = pid2slotId(page.getId());
                // Add new pages into buffer pool
                if (slotId == null) {
                    pages[getSlotIdAndUpdatePageTable(page.getId())] = page;
                }
                page.markDirty(true, tid);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile dbFile = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> mPages = dbFile.deleteTuple(tid, t);
        synchronized (metaLatch) {
            for (Page page : mPages) {
                Integer slotId = pid2slotId(page.getId());
                // Add new pages into buffer pool
                if (slotId == null) {
                    pages[getSlotIdAndUpdatePageTable(page.getId())] = page;
                }
                page.markDirty(true, tid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public void flushAllPages() throws IOException {
        synchronized (metaLatch) {
            for (PageId pageId : pageTable.keySet()) {
                flushPage(pageId);
            }
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public void discardPage(PageId pid) {
        synchronized (metaLatch) {
            if (!pageTable.containsKey(pid)) {
                // TODO(JACK ZHANG): 2021/11/1 Or we should do nothing?
                throw new RuntimeException(String.format("Page{%s} is not in buffer pool", pid));
            }
            Integer slotId = pid2slotId(pid);
            // Remove from page table
            pageTable.remove(pid);
            // Add into available slotIds
            availableSlots.add(slotId);
        }
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private void flushPage(PageId pid) throws IOException {
        Page page;
        synchronized (metaLatch) {
            if (!pageTable.containsKey(pid)) {
                // TODO(JACK ZHANG): 2021/11/1 Or we should do nothing?
                throw new RuntimeException(String.format("Page{%s} is not in buffer pool", pid));
            }
            Integer slotId = pid2slotId(pid);
            page = pages[slotId];
        }
        // flush page to disk
        page.markDirty(false, null);
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public void flushPages(TransactionId tid) throws IOException {
        synchronized (metaLatch) {
            for (PageId pid : pageTable.keySet()) {
                if (Objects.equals(tid, pages[pid2slotId(pid)].isDirty())) {
                    flushPage(pid);
                }
            }
        }
    }


    /**
     * Write all pages of the specified transaction to disk.
     */
    public void restorePages(TransactionId tid) {
        synchronized (metaLatch) {
            for (PageId pid : pageTable.keySet()) {
                if (Objects.equals(tid, pages[pid2slotId(pid)].isDirty())) {
                    Page pageFromDisk = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                    pages[pid2slotId(pid)] = pageFromDisk;
                }
            }
        }
    }

    /**
     * Get slotId from pageId
     * @param pid page id
     * @return slot id
     */
    private Integer pid2slotId(PageId pid) {
        return pageTable.get(pid);
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * @implSpec  This method should be sync by metaLock.
     */
    private void evictPage() throws DbException {
        PageId pid = null;
        Iterator<PageId> iterator = lruPids.iterator();
        while (iterator.hasNext()) {
            PageId next = iterator.next();
            Integer slotId = pid2slotId(next);
            if (slotId != null && pages[slotId].isDirty() == null) {
                pid = next;
                iterator.remove();
                break;
            }
        }
        if (pid == null) {
            throw new DbException("No page to evict, all pages are dirty");
        }
        try {
            flushPage(pid);
            discardPage(pid);
            Database.getLockManager().unlock(pid);
        } catch (IOException e) {
            throw new DbException(String.format("Flush page{%s} failed when eviction", pid));
        }
    }

    private Set<PageId> getPidsInTable() {
        synchronized (metaLatch) {
            return pageTable.keySet();
        }
    }

}
