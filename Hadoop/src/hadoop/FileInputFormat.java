package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;

public class FileInputFormat {
	
	

	public static void addInputPath(Job job, String inputDir) throws IOException, NoSuchMethodException, SecurityException
	{
		job.INPUT_DIR = inputDir;
	}
	
	
}
