package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, Integer> countMap = new HashMap<>();
    private int count = 0;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if (what != Op.COUNT) {
            throw new IllegalArgumentException("string aggregation operator only support COUNT");
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field groupByField = tup.getField(gbfield);
        // update count
        ++count;
        countMap.put(groupByField, 1 + (countMap.getOrDefault(groupByField, 0)));
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if (needGroupBy()) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        List<Tuple> tuples = new ArrayList<>();
        Tuple tuple;
        if (needGroupBy()) {
            tuple = new Tuple(td);
            tuple.setField(0, new IntField(count));
            tuples.add(tuple);
        } else {
            for (Map.Entry<Field, Integer> entry : countMap.entrySet()) {
                tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, new IntField(entry.getValue()));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    private boolean needGroupBy() {
        return gbfield == NO_GROUPING;
    }

}
