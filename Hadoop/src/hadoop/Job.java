package hadoop;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

public class Job {

	private String jobname;
	public Object mapperInstance;
	public Class<?> mapperCls;
	public Class<?> reducerCls;
	public Object reducerInstance;
	private Class jar;
	private static Job job;
	public int NUM_REDUCE_TASKS;
	public int MAPPER_TASKS = 0;
	public int REDUCER_TASKS = 0;
	public Configuration conf;
	
	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}
	
	public static Job getInstance(Configuration conf,String jobname)
	{
		job = new Job();
		job.jobname = jobname;
		job.conf = conf;
		
		return job;
	}
	
	public void setMapperClass(Class<? extends Mapper> mapperClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		mapperCls = Class.forName(mapperClass.getName());
		mapperInstance = mapperCls.newInstance();
	}
	
	public void setNumReduceTasks(int reduceTasks)
	{
		job.NUM_REDUCE_TASKS = reduceTasks;
	}
	public void setReducerClass(Class<? extends Reducer> reducerClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		reducerCls = Class.forName(reducerClass.getName());
		reducerInstance = reducerCls.newInstance();
		
	}
	
	public void setJarByClass(Class jarClass) throws ClassNotFoundException
	{
		ClassLoader classLoader = jarClass.getClassLoader();
		job.jar = classLoader.loadClass(jarClass.getName());
	}
	
	public void mapperTask(File inputFile) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		
		job.MAPPER_TASKS++;
		System.out.println("Mapper task "+job.MAPPER_TASKS+" started");
		FileReader fileReader = new FileReader(inputFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = Object.class;
		cArgs[1] = String.class;
		cArgs[2] = Context.class;
				
		Method mapMethod = mapperCls.getMethod("map",cArgs);
		
		Context mapContext = new Context();
		String temp_map_file = job.conf.prop.getProperty("TEMP_DIR")+"part-temp-"+job.MAPPER_TASKS;
		mapContext.setup(temp_map_file);
		while((line = bufferedReader.readLine())!= null)
		{
			mapMethod.invoke(mapperInstance,new Object(),line,mapContext);
			
		}
		
		bufferedReader.close();
		System.out.println("Mapper task "+job.MAPPER_TASKS+" completed");
	}
	
	public void reducerTask(File inputFile) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	{
		
		job.REDUCER_TASKS++;
		System.out.println("Reducer task "+job.REDUCER_TASKS+" started");
		FileReader fileReader = new FileReader(inputFile);
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = String.class;
		cArgs[1] = ArrayList.class;
		cArgs[2] = Context.class;
				
		Method reduceMethod = reducerCls.getMethod("reduce",cArgs);
		Context reduceContext = new Context();
		String part_output_file = job.conf.prop.getProperty("OUTPUT_DIR")+"part-r-0000"+job.REDUCER_TASKS;
		reduceContext.setup(part_output_file);
		
		HashMap<String, ArrayList<Integer>> wordMap = new HashMap<>();
		
		line = bufferedReader.readLine();
		
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
		
		for(String key : wordMap.keySet())
		{
			reduceMethod.invoke(reducerInstance,key,wordMap.get(key),reduceContext);
		}
		
		System.out.println("Reducer task "+job.REDUCER_TASKS+" completed");
	}
	
	public void cleanDirectory(File directory)
	{
		for(File f : directory.listFiles())
			f.delete();
		
	}
	public void cleanUpFiles()
	{
		cleanDirectory(new File(job.conf.prop.getProperty("TEMP_DIR")));
		cleanDirectory(new File(job.conf.prop.getProperty("OUTPUT_DIR")));
		
	}
	
	public int waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		
		int status = -1;
		
		cleanUpFiles();
		
		File files = new File(job.conf.prop.getProperty("INPUT_DIR"));
		for(File file :files.listFiles())
		{
			mapperTask(file);
		}
		
		File temp_folder = new File(job.conf.prop.getProperty("TEMP_DIR"));
		for(File file : temp_folder.listFiles())
		{
			reducerTask(file);
		}		
		
		return status;
	}
}
