package hadoop;

public class IntWritable extends DataType{

	private int value;

	public IntWritable() {}

	public IntWritable(int value) { set(value); }

	/** Set the value of this IntWritable. */
	public void set(int value) { this.value = value; }

	/** Set the value of this IntWritable. */
	public void set(Integer value) { this.value = value; }

	/** Return the value of this IntWritable. */
	public int get() { return value; }
	
	@Override
	public String toString() {
		return ""+value;
	}
}
