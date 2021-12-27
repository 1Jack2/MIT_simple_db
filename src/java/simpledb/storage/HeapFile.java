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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
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

        RandomAccessFile f = null;
        try{
            f = new RandomAccessFile(file,"r");
            if((long) (pgNo + 1) *BufferPool.getPageSize() > f.length()){
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek((long) pgNo * BufferPool.getPageSize());
            // big end
            int read = f.read(bytes,0,BufferPool.getPageSize());
            if(read != BufferPool.getPageSize()){
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(),pid.getPageNumber());
            return new HeapPage(id, bytes);
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try{
                if (f != null) {
                    f.close();
                }
            }catch (Exception e){
                e.printStackTrace();
            }
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

    private void insertTupleToNewPage(Tuple t, ArrayList<Page> mPageList, HeapPageId pid) throws IOException, DbException {
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
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> mPageList = new ArrayList<>();
        mPageList.add(page);
        return mPageList;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int whichPage;

        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }
        @Override
        public void open() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException{
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            if(it == null){
                return false;
            }

            if(!it.hasNext()){
                if(whichPage < (heapFile.numPages()-1)){
                    whichPage++;
                    it = getPageTuples(whichPage);
                    return hasNext();
                }else{
                    return false;
                }
            }else{
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            // TODO Auto-generated method stub
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // TODO Auto-generated method stub
            close();
            open();
        }

        @Override
        public void close() {
            // TODO Auto-generated method stub
            it = null;
        }

    }
}

