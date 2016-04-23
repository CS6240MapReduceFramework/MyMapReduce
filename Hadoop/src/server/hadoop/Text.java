package hadoop;

public class Text extends DataType{

	String str;
	
	public void set(String str)
	{
		this.str = str;
	}
	
	public String get()
	{
		return this.str;
	}
	
	@Override
	public String toString()
	{
		return this.str;
	}
}
