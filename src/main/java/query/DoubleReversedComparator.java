package query;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

// The code from https://gist.github.com/geofferyzh/3839714 was appropriated for descending double sort in MapReduce
public class DoubleReversedComparator extends WritableComparator {
    protected DoubleReversedComparator() {
        super(DoubleWritable.class, true);
    }

    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        DoubleWritable k1 = (DoubleWritable) w1;
        DoubleWritable k2 = (DoubleWritable) w2;

        return -k1.compareTo(k2);
    }
}
