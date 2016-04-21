package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

public class MapperThread implements Runnable{

	
	private Job job;
	private File file;
	public String status;
	
	MapperThread(Job job,File file)
	{
		this.job = job;
		this.file = file;
		this.status = "DEFINED";
	}
	@Override
	public void run() {
		
		try {
			mapperTask(file);
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void mapperTask(File inputFile) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		status = "STARTED";
		
		job.MAPPER_TASKS++;
		//System.out.println(Thread.currentThread().getName()+" started");
		FileReader fileReader = new FileReader(inputFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = Object.class;
		cArgs[1] = String.class;
		cArgs[2] = Context.class;
				
		Method mapMethod = job.mapperCls.getMethod("map",cArgs);
		
		Context mapContext = new Context();
		System.out.println("thread name: "+ Thread.currentThread().getName());
		String temp_map_file = job.conf.prop.getProperty("TEMP_DIR")+"part-temp-"+Thread.currentThread().getName();
		//mapContext.setup(temp_map_file);
		while((line = bufferedReader.readLine())!= null)
		{
			mapMethod.invoke(job.mapperInstance,new Object(),line,mapContext);
			
		}
		
		bufferedReader.close();
		status = "COMPLETED";
		//System.out.println(Thread.currentThread().getName()+" completed");
	}
	
	

}
