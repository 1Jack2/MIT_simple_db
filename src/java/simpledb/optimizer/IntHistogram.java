package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int[] buckets;
    private final int maxValue;
    private final int minValue;
    private final int width;
    private int totalTuples = 0;
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
        this.minValue = min;
        this.maxValue = max;
        this.buckets = new int[buckets];
        this.width = ((max - min + 1) + (buckets - 1)) / buckets;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	if (outOfRange(v)) {
            throw new IllegalArgumentException(String.format("%d is out of legal range [%d, %d]",
                    v, getMinValue(), getMaxValue()));
        }
        incBucketHeight(v);
        incTotalTuples();
    }

    private boolean outOfRange(int v) {
        return tooLarge(v) || tooSmall(v);
    }

    private boolean tooLarge(int v) {
        return v > getMaxValue();
    }

    private boolean tooSmall(int v) {
        return v < getMinValue();
    }

    /**
     *
     * @param v Value to add to the histogram or to compare with
     * @return The height of the {@code v}'s corresponding bucket
     */
    private int getBucketHeight(int v) {
        return buckets[getBucketIndex(v)];
    }

    /**
     *
     * @param v Value to add to the histogram or to compare with
     * @return The index of the {@code v}'s corresponding bucket
     */
    private int getBucketIndex(int v) {
        return (v - getMinValue()) / getWidth();
    }

    /**
     *
     * @param v Value to add to the histogram or to compare with
     * @return The right border of the {@code v}'s corresponding bucket
     */
    private int getRightBorder(int v) {
        return (getMinValue() + getWidth()) + getBucketIndex(v) * getWidth();
    }

    /**
     *
     * @param v Value to add to the histogram or to compare with
     * @return The left border of the {@code v}'s corresponding bucket
     */
    private int getLeftBorder(int v) {
        return getMinValue() + getBucketIndex(v) * getWidth();
    }

    /**
     *
     * @param v Value to add to the histogram or to compare with
     */
    private void setBucketHeight(int v, int value) {
        buckets[(getBucketIndex(v))] = value;
    }

    /**
     * @param v Value to add to the histogram or to compare with
     */
    private void incBucketHeight(int v) {
        setBucketHeight(v, getBucketHeight(v) + 1);
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        double result;
        switch (op) {
            case EQUALS:
                if (outOfRange(v)) {
                    return 0d;
                }
                // No break here
            case NOT_EQUALS:
                if (outOfRange(v)) {
                    return 1d;
                }

                result = ((double) getBucketHeight(v)) / getWidth() / totalTuples;

                if (op == Predicate.Op.NOT_EQUALS) {
                    result = 1 - result;
                }
                break;
            case LESS_THAN_OR_EQ:
                if (tooSmall(v)) {
                    return 0d;
                }
                if (tooLarge(v)) {
                    return 1d;
                }
                // No break here
            case GREATER_THAN:
                if (tooSmall(v)) {
                    return 1d;
                }
                if (tooLarge(v)) {
                    return 0d;
                }

                double rightCnt = 0d;
                for (int i = getBucketIndex(v) + 1; i < getBuckets().length; i++) {
                    rightCnt += buckets[i];
                }
                result = (rightCnt + getBucketHeight(v) * Math.max(0d, getRightBorder(v) - (v + 1))) / getWidth()
                        / getTotalTuples();

                if (op == Predicate.Op.LESS_THAN_OR_EQ) {
                    result = 1 - result;
                }
                break;
            case GREATER_THAN_OR_EQ:
                if (tooSmall(v)) {
                    return 1d;
                }
                if (tooLarge(v)) {
                    return 0d;
                }
                // No break here
            case LESS_THAN:
                if (tooSmall(v)) {
                    return 0d;
                }
                if (tooLarge(v)) {
                    return 1d;
                }

                double leftCnt = 0;
                for (int i = getBucketIndex(v) - 1; i >= 0; i--) {
                    leftCnt += buckets[i];
                }
                result = (leftCnt + getBucketHeight(v) * Math.max(0d, (v - 1) - getLeftBorder(v))) / getWidth()
                        / getTotalTuples();

                if (op == Predicate.Op.GREATER_THAN_OR_EQ) {
                    result = 1 - result;
                }
                break;
            default:
                throw new IllegalStateException("Unsupported Operation: " + op);
        }
        return result;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    public int[] getBuckets() {
        return buckets;
    }

    public int getMaxValue() {
        return maxValue;
    }

    public int getMinValue() {
        return minValue;
    }

    public int getWidth() {
        return width;
    }

    public int getTotalTuples() {
        return totalTuples;
    }

    private void setTotalTuples(int totalTuples) {
        this.totalTuples = totalTuples;
    }

    private void incTotalTuples() {
        setTotalTuples(getTotalTuples() + 1);
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + Arrays.toString(buckets) +
                ", maxValue=" + maxValue +
                ", minValue=" + minValue +
                ", width=" + width +
                '}';
    }
}
