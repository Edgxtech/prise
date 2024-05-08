package tech.edgx.prise.indexer.util;

import org.apache.commons.math.MathException;
import org.apache.commons.math.distribution.TDistributionImpl;
import org.apache.commons.math.stat.StatUtils;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class StatsUtil {

    /** default significance level */
    public static final double DEFAULT_SIGNIFICANCE_LEVEL = 0.85; // 0.95, E.g. 2 std deviations,

    public boolean hasOutlier(Object ... values) {
        return hasOutlier(Arrays.asList(values));
    }
    public boolean hasOutlier(List<?> values) {
        return getOutlier(values) != null;
    }

    /**
     * Returns a statistical outlier with the default significance level (0.95),
     * or null if no such outlier exists..
     */
    public static <T> T getOutlier(List<T> values) {
        return getOutlier(values, DEFAULT_SIGNIFICANCE_LEVEL);
    }
    public static <T> T getOutlier(List<T> values, double significanceLevel) {
        AtomicReference<T> outlier = new AtomicReference<T>();
        double grubbs = getGrubbsTestStatistic(values, outlier);
        double size = values.size();
        if(size < 3) {
            return null;
        }
        TDistributionImpl t = new TDistributionImpl(size - 2.0);
        try {
            double criticalValue = t.inverseCumulativeProbability((1.0 - significanceLevel) / (2.0 * size));
            double criticalValueSquare = criticalValue * criticalValue;
            double grubbsCompareValue = ((size - 1) / Math.sqrt(size)) *
                    Math.sqrt((criticalValueSquare) / (size - 2.0 + criticalValueSquare));
            if(grubbs > grubbsCompareValue) {
                return outlier.get();
            } else {
                return null;
            }
        } catch (MathException e) {
            throw new RuntimeException(e);
        }
    }
    /** returns a minimum outlier (if one exists) */
    public <T> T getOutlierMin(List<T> values) {
        T d = getOutlier(values, DEFAULT_SIGNIFICANCE_LEVEL);
        if(d == null)
            return null;
        double d1 = toDouble(d);
        double d2 = toDouble(min(values));
        if(d1 == d2)
            return d;
        return null;
    }
    /** returns a minimum outlier (if one exists) */
    public static <T> T getOutlierMax(List<T> values) {
        T d = getOutlier(values, DEFAULT_SIGNIFICANCE_LEVEL);
        if(d == null)
            return null;
        double d1 = toDouble(d);
        double d2 = toDouble(max(values));
        if(d1 == d2)
            return d;
        return null;
    }

    public static <T> double getGrubbsTestStatistic(List<T> values, AtomicReference<T> outlier) {
        double[] array = toArray(values);
        double mean = StatUtils.mean(array);
        double stddev = stdDev(values);
        double maxDev = 0;
        for(T o : values) {
            double d = toDouble(o);
            if(Math.abs(mean - d) > maxDev) {
                maxDev = Math.abs(mean - d);
                outlier.set(o);
            }
        }
        double grubbs = maxDev / stddev;
        return grubbs;
    }

    private static enum Operator {
        MIN, MAX
    }

    public static double sum(Collection<?> c) {
        double s = 0;
        for(Object o : c) {
            s += Double.parseDouble("" + o);
        }
        return s;
    }

    public static double average(Collection<?> c) {
        return sum(c) / (double) c.size();
    }
    public static double avg(Collection<?> c) {
        return average(c);
    }

    public static <T> T min(List<T> values) {
        return executeOp(values, Operator.MIN);
    }
    public static <T> T max(List<T> values) {
        return executeOp(values, Operator.MAX);
    }
    public static double max(double[] values) {
        return executeOp(asList(values), Operator.MAX);
    }
    public static int max(int[] values) {
        return executeOp(asList(values), Operator.MAX);
    }
    public static long max(long[] values) {
        return executeOp(asList(values), Operator.MAX);
    }

    private static <T> T executeOp(List<T> values, Operator op) {
        double res = op == Operator.MIN ? Double.MAX_VALUE : Double.MIN_VALUE;
        T obj = null;
        for(T o : values) {
            double d = toDouble(o);
            if((op == Operator.MIN && d < res) ||
                    (op == Operator.MAX && d > res)){
                res = d;
                obj = o;
            }
        }
        return obj;
    }

    public static List<Integer> asList(int[] values) {
        List<Integer> result = new LinkedList<Integer>();
        for(int v : values) {
            result.add(v);
        }
        return result;
    }
    public static List<Long> asList(long[] values) {
        List<Long> result = new LinkedList<Long>();
        for(long v : values) {
            result.add(v);
        }
        return result;
    }
    public static List<Double> asList(double[] values) {
        List<Double> result = new LinkedList<Double>();
        for(double v : values) {
            result.add(v);
        }
        return result;
    }

    public static Double stdDev(List<?> values) {
        return stdDev(toArray(values));
    }
    public static Double stdDev(double[] values) {
        return Math.sqrt(StatUtils.variance(values));
    }

    public static Double toDouble(Object o) {
        if(o instanceof Double)
            return (Double)o;
        if(o instanceof Integer)
            return (double)(int)(Integer)o;
        if(o instanceof Long)
            return (double)(long)(Long)o;
        return Double.parseDouble("" + o);
    }
    public static List<Double> toDoubles(List<?> values) {
        List<Double> d = new LinkedList<Double>();
        for(Object o : values) {
            double val = toDouble(o);
            d.add(val);
        }
        return d;
    }
    public static double[] toArray(List<?> values) {
        double[] d = new double[values.size()];
        int count = 0;
        for(Object o : values) {
            double val = o instanceof Double ? (Double)o : Double.parseDouble("" + o);
            d[count++] = val;
        }
        return d;
    }

    public static Integer toInteger(String in) {
        if(in == null)
            return null;
        try {
            return Integer.parseInt(in);
        } catch(Exception e) {
            return null;
        }
    }

    private static void assertion(boolean value) {
        if(!value)
            throw new RuntimeException("Assertion failed.");
    }

    /// ALT
    public static List<Double> getOutliers(List<Double> input) {
        List<Double> output = new ArrayList<Double>();
        List<Double> data1 = new ArrayList<Double>();
        List<Double> data2 = new ArrayList<Double>();
        if (input.size() % 2 == 0) {
            data1 = input.subList(0, input.size() / 2);
            data2 = input.subList(input.size() / 2, input.size());
        } else {
            data1 = input.subList(0, input.size() / 2);
            data2 = input.subList(input.size() / 2 + 1, input.size());
        }
        double q1 = getMedian(data1);
        double q3 = getMedian(data2);
        double iqr = q3 - q1;
        double lowerFence = q1 - 1.5 * iqr;
        double upperFence = q3 + 1.5 * iqr;
        for (int i = 0; i < input.size(); i++) {
            if (input.get(i) < lowerFence || input.get(i) > upperFence)
                output.add(input.get(i));
        }
        return output;
    }

    private static double getMedian(List<Double> data) {
        if (data.size() % 2 == 0)
            return (data.get(data.size() / 2) + data.get(data.size() / 2 - 1)) / 2;
        else
            return data.get(data.size() / 2);
    }
}