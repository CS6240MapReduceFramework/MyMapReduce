package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;

public class ReducerThread implements Runnable{

	private Job job;
	public String threadName;
	public int startIndex;
	public int endIndex;
	public int no_of_files;
	public String status;
	
	public ReducerThread(Job job)
	{
		this.job = job;
		this.status = "DEFINED";
	}
	public void reducerTask() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	{

		status = "STARTED";

		Class[] cArgs = new Class[3];
		cArgs[0] = String.class;
		cArgs[1] = ArrayList.class;
		cArgs[2] = Context.class;
		Method reduceMethod = job.reducerCls.getMethod("reduce",cArgs);
		Context reduceContext = new Context();
		String part_output_file = job.conf.prop.getProperty("OUTPUT_DIR")+"part-r-0000"+Thread.currentThread().getName();
		//reduceContext.setup(part_output_file);
		HashMap<String, ArrayList<Integer>> wordMap = new HashMap<>();

		for(int i=startIndex;i<= endIndex;i++)
		{
			FileReader fileReader = new FileReader(job.partFiles[i]);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line = null;
			while((line = bufferedReader.readLine())!= null)
			{
				String[] lines = line.split(" ");
				if(wordMap.containsKey(lines[0]))
				{
					ArrayList<Integer> list = wordMap.get(lines[0]);
					list.add(1);
					wordMap.put(lines[0],list);
				}
				else
				{
					ArrayList<Integer> list = new ArrayList<Integer>();
					list.add(1);
					wordMap.put(lines[0], list);
				}
			}

			bufferedReader.close();

		}


		for(String key : wordMap.keySet())
		{
			reduceMethod.invoke(job.reducerInstance,key,wordMap.get(key),reduceContext);
		}
		status = "COMPLETED";
	}
	
	public void run()
	{
		try {
			reducerTask();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
