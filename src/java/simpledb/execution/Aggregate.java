package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /** The OpIterator that is feeding us tuples. */
    private OpIterator child;
    /** The column over which we are computing an aggregate */
    private final int afield;
    /** The column over which we are grouping the result, or -1 if there is no grouping */
    private final int gfield;
    /** The aggregation operator to use */
    private final Aggregator.Op aOp;

    private OpIterator opIterator;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aOp = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return aOp;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        child.open();

        // init aggregator
        TupleDesc td = child.getTupleDesc();
        Type gbfieldType = notNeedGroupBy() ? null : td.getFieldType(gfield);
        Type afieldType = td.getFieldType(afield);
        Aggregator aggregator;
        switch (afieldType) {
            case INT_TYPE:
                aggregator = new IntegerAggregator(gfield, gbfieldType, afield, aOp);
                break;
            case STRING_TYPE:
                aggregator = new StringAggregator(gfield, gbfieldType, afield, aOp);
                break;
            default:
                throw new IllegalStateException("unsupported aggregate on type: " + afieldType);
        }
        // do aggregation
        while (child.hasNext()) {
            aggregator.mergeTupleIntoGroup(child.next());
        }
        // get iterator
        opIterator = aggregator.iterator();
        opIterator.open();

        super.open();
    }

    private boolean notNeedGroupBy() {
        return Aggregator.NO_GROUPING == gfield;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (opIterator == null) {
            throw new IllegalStateException("Aggregator not yet open");
        }
        return opIterator.hasNext() ? opIterator.next() : null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        TupleDesc.TDItem[] tdItems = child.getTupleDesc().getTdItems();
        TupleDesc.TDItem aTdItem = tdItems[aggregateField()];
        TupleDesc.TDItem gTdItem = tdItems[groupField()];
        // FIXME: 4/10/22 javadoc与测试AggregageTest不一致
//        TupleDesc.TDItem aTdItemNew = new TupleDesc.TDItem(aTdItem.fieldType,
//                String.format("%s(%s)", nameOfAggregatorOp(aOp), aTdItem.fieldName));
        TupleDesc.TDItem aTdItemNew = new TupleDesc.TDItem(aTdItem.fieldType, gTdItem.fieldName);
        return new TupleDesc(new TupleDesc.TDItem[]{gTdItem, aTdItemNew});
    }

    public void close() {
        super.close();
        opIterator.close();
        opIterator = null;
        child.close();
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
