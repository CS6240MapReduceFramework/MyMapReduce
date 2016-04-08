package hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Context {

	
	private static BufferedWriter bufferedWriter;
	private static FileWriter fileWriter;
	
	public void setup(String fileName) throws IOException
	{
		File f = new File(fileName);
		if(!f.exists())
			f.createNewFile();
		
		fileWriter = new FileWriter(fileName,true);
	}
	public void write(String key, Integer value) throws IOException
	{
		bufferedWriter = new BufferedWriter(fileWriter);
		bufferedWriter.write(key+" "+value+"\n");
		bufferedWriter.flush();
	}
}
