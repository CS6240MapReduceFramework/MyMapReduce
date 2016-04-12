package hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class DoubleWritable implements WritableComparable<DoubleWritable> {

	private double value = 0.0;

	public DoubleWritable() {}

	public DoubleWritable(double value) { set(value); }

	/** Set the value of this DoubleWritable. */
	public void set(double value) { this.value = value; }

	/** Return the value of this DoubleWritable. */
	public double get() { return value; }

	@Override
	public void readFields(DataInput in) throws IOException {
		value = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(value);
	}

	@Override
	public boolean equals(Object o) {
		if (!(o instanceof DoubleWritable))
			return false;
		DoubleWritable other = (DoubleWritable)o;
		return this.value == other.value;
	}

	@Override
	public int hashCode() {
		return (int)Double.doubleToLongBits(value);
	}

	/** Compares two DoubleWritables. */
	@Override
	public int compareTo(DoubleWritable o) {
		return (value < o.value ? -1 : (value == o.value ? 0 : 1));
	}

	@Override
	public String toString() {
		return Double.toString(value);
	}

//	/** A Comparator optimized for DoubleWritable. */ 
//	public static class Comparator extends WritableComparator {
//		public Comparator() {
//			super(DoubleWritable.class);
//		}
//
//		@Override
//		public int compare(byte[] b1, int s1, int l1,
//				byte[] b2, int s2, int l2) {
//			double thisValue = readDouble(b1, s1);
//			double thatValue = readDouble(b2, s2);
//			return (thisValue < thatValue ? -1 : (thisValue == thatValue ? 0 : 1));
//		}
//	}
//
//	static {                                        // register this comparator
//		WritableComparator.define(DoubleWritable.class, new Comparator());
//	}

}
