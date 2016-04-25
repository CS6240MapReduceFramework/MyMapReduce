package hadoop;


public class DoubleWritable extends DataType{

	private double value = 0.0;

	public DoubleWritable() {}

	public DoubleWritable(double value) { set(value); }

	/** Set the value of this DoubleWritable. */
	public void set(double value) { this.value = value; }

	/** Return the value of this DoubleWritable. */
	public double get() { return value; }

	@Override
	public String toString() {
		return Double.toString(value);
	}
}

