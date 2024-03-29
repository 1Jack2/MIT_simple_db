package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /** the file that stores the on-disk backing store for this heap file */
    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        try (RandomAccessFile f = new RandomAccessFile(file, "r")) {
            if (((long) (pgNo + 1)) * BufferPool.getPageSize() > f.length()) {
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] buf = new byte[BufferPool.getPageSize()];
            f.seek(((long) pgNo) * BufferPool.getPageSize());
            // big end
            int read = f.read(buf, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, buf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        RandomAccessFile f = new RandomAccessFile(file, "rw");
        f.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
        f.write(page.getPageData(), 0, BufferPool.getPageSize());
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) (file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> mPageList = new ArrayList<>();
        if (numPages() == 0) {
            Files.write(Paths.get(file.getAbsolutePath()), new byte[BufferPool.getPageSize()], StandardOpenOption.APPEND);
        }
        HeapPageId pid = new HeapPageId(getId(), 0);
        HeapPage page = null;
        //region Find a not full page
        for (int i = 0; i < numPages(); i++) {
            pid.setPageNo(i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                break;
            } else {
                Database.getBufferPool().unsafeReleasePage(tid, page.getId());
            }
        }
        //endregion
        try {
            if (page == null) {
                insertTupleToNewPage(t, mPageList, pid);
            } else {
                page.insertTuple(t);
            }
        } catch (DbException e) {
            insertTupleToNewPage(t, mPageList, pid);
        }
        mPageList.add(page);
        return mPageList;
    }

    /**
     * Inserts a tuple into a new page.
     *
     * lab4 exercise2:
     * Adding a new page to a HeapFile. When do you physically write the page to disk? Are there race conditions
     * with other transactions (on other threads) that might need special attention at the HeapFile level,
     * regardless of page-level locking?
     */
    private synchronized void insertTupleToNewPage(Tuple t, ArrayList<Page> mPageList, HeapPageId pid) throws IOException, DbException {
        // create a new page and append it to the physical file on disk
        HeapPage newPage = new HeapPage(new HeapPageId(pid.getTableId(), pid.getPageNumber() + 1),
                new byte[BufferPool.getPageSize()]);
        // update recordID
        t.setRecordId(new RecordId(newPage.getId(), 0));
        try {
            newPage.insertTuple(t);
        } catch (DbException e) {
            throw new DbException("Insert a tuple to a new page fails");
        }
        mPageList.add(newPage);
        Files.write(Paths.get(file.getAbsolutePath()), newPage.getPageData(), StandardOpenOption.APPEND);
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return Collections.singletonList(page);
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator {
        private final HeapFile heapFile;
        private final TransactionId tid;

        private Iterator<Tuple> it;
        private int pageNo;

        public HeapFileIterator(HeapFile file, TransactionId tid) {
            this.heapFile = file;
            this.tid = tid;
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            pageNo = 0;
            it = pageIterator(pageNo);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (it == null) return false;
            if (it.hasNext()) return true;
            // no more tuples in current page, try next page
            ++pageNo;
            if (!(pageNo >= 0 && pageNo < heapFile.numPages())) return false;

            try {
                HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
                // Acquire page lock
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                // Update iterator
                it = page.iterator();
                if (it.hasNext())
                    return true;
                // Release page lock.
                // Although this apparently contradicts the rules of two-phase locking, it is ok because
                // t did not use any data from the page, such that a concurrent transaction t' which updated p cannot
                // possibly effect the answer or outcome of t.
                Database.getBufferPool().unsafeReleasePage(tid, page.getId());
            } catch (DbException e) {
                System.out.println(e.getMessage());
            }

            return false;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (it == null || !it.hasNext()) {
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }

        /**
         * Returns an iterator over a specified page of this HeapFile.
         */
        private Iterator<Tuple> pageIterator(int pageNo) throws TransactionAbortedException, DbException {
            HeapPageId pid = new HeapPageId(heapFile.getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
            return page.iterator();
        }
    }
}

