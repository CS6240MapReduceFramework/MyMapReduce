package hadoop;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class Context {

	private static String TEMP_DIR ;
	private static BufferedWriter bufferedWriter;
	private static FileWriter fileWriter;
	
	public void setup(String temp_dir) throws IOException
	{
		TEMP_DIR = temp_dir;
		fileWriter = new FileWriter(TEMP_DIR);
		bufferedWriter = new BufferedWriter(fileWriter);
	}
	public void write(String key, Integer value) throws IOException
	{
		bufferedWriter.write(key+" "+value+"\n");
	}
}
