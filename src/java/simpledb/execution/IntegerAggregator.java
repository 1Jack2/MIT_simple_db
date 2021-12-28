package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private final Map<Field, Integer> resultMap = new HashMap<>();
    private final Map<Field, Integer> countMap = new HashMap<>();
    private int result = 0;
    private int count = 0;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field aggregateField = tup.getField(afield);
        int aValue = ((IntField) aggregateField).getValue();
        Field groupByField = notNeedGroupBy() ? null : tup.getField(gbfield);
        // update count
        ++count;
        countMap.put(groupByField, 1 + (countMap.getOrDefault(groupByField, 0)));
        switch (what) {
            case MIN:
                if (notNeedGroupBy()) {
                    result = Math.min(aValue, result);
                } else {
                    resultMap.put(groupByField, Math.min(resultMap.getOrDefault(groupByField, Integer.MAX_VALUE), aValue));
                }
                break;
            case MAX:
                if (notNeedGroupBy()) {
                    result = Math.max(aValue, result);
                } else {
                    resultMap.put(groupByField, Math.max(resultMap.getOrDefault(groupByField, Integer.MIN_VALUE), aValue));
                }
                break;
            case SUM:
                // no break
            case AVG:
                if (notNeedGroupBy()) {
                    result += aValue;
                } else {
                    resultMap.put(groupByField, resultMap.getOrDefault(groupByField, 0) + aValue);
                }
                break;
            case COUNT:
                if (notNeedGroupBy()) {
                    result = count;
                } else  {
                    resultMap.put(groupByField, countMap.getOrDefault(groupByField, 0));
                }
                break;
            default:
                throw new IllegalStateException("unsupported aggregation operator");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        TupleDesc td;
        if (notNeedGroupBy()) {
            td = new TupleDesc(new Type[]{Type.INT_TYPE});
        } else {
            td = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
        }
        List<Tuple> tuples = new ArrayList<>();
        Tuple tuple;
        if (notNeedGroupBy()) {
            tuple = new Tuple(td);
            if (what == Op.AVG) {
                tuple.setField(0, new IntField(result / count));
            } else {
                tuple.setField(0, new IntField(result));
            }
            tuples.add(tuple);
        } else {
            for (Map.Entry<Field, Integer> entry : resultMap.entrySet()) {
                tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                if (what == Op.AVG) {
                    tuple.setField(1, new IntField(entry.getValue() / countMap.get(entry.getKey())));
                } else {
                    tuple.setField(1, new IntField(entry.getValue()));
                }
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }

    private boolean notNeedGroupBy() {
        return gbfield == NO_GROUPING;
    }

}
