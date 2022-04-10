package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /** The child operator from which to read tuples to be inserted. */
    private OpIterator child;
    /** The table in which to insert tuples */
    private final int tableId;
    /** The transaction running the insert */
    private final TransactionId tid;

    private final TupleDesc OPERATOR_TD = new TupleDesc(new Type[]{Type.INT_TYPE});
    private final TupleDesc tableTD;

    // Should be used once
    private boolean used = false;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        this.tableTD =  Database.getCatalog().getTupleDesc(tableId);
    }

    public TupleDesc getTupleDesc() {
        return OPERATOR_TD;
    }

    public TupleDesc getTableTupleDesc() {
        return tableTD;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        used = false;
        super.open();
    }

    public void close() {
        super.close();
        used = true;
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (used) return null;
        used = true;

        int inserted = 0;
        while (child.hasNext()) {
            try {
                Tuple next = child.next();
                next.resetTupleDesc(getTableTupleDesc());
                Database.getBufferPool().insertTuple(tid, tableId, next);
                ++inserted;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        Tuple tuple = new Tuple(getTupleDesc());
        tuple.setField(0, new IntField(inserted));
        return tuple;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }
}
