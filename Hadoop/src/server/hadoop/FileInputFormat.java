package hadoop;

import java.io.IOException;

public class FileInputFormat {
	
	public static void addInputPath(Job job, String inputDir) throws IOException, NoSuchMethodException, SecurityException
	{
		job.conf.prop.setProperty("INPUT_DIR", inputDir);
	}	
}
