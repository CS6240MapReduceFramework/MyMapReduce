package hadoop;

import java.io.BufferedReader;
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
	public  String INPUT_DIR;
	
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
		
		return job;
	}
	
	public void setMapperClass(Class<? extends Mapper> mapperClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
//		ClassLoader classLoader = mapperClass.getClassLoader();
//		job.mapper = classLoader.loadClass(mapperClass.getName());
		mapperCls = Class.forName(mapperClass.getName());
		mapperInstance = mapperCls.newInstance();
		
		
//		Method method = job.mapper.getMethod("map");
//		method.invoke(null);	
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
	
	public void mapperTask() throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		FileReader fileReader = new FileReader(job.INPUT_DIR+"/alice.txt");
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = Object.class;
		cArgs[1] = String.class;
		cArgs[2] = Context.class;
				
		Method mapMethod = mapperCls.getMethod("map",cArgs);
		
		System.out.println("got Method map from Mapper class: "+mapMethod.toString());
		Context mapContext = new Context();
		mapContext.setup("/home/kaushikveluru/Desktop/Project/alice_map_output.txt");
		while((line = bufferedReader.readLine())!= null)
		{
			mapMethod.invoke(mapperInstance,new Object(),line,mapContext);
			
		}
		
		bufferedReader.close();
	}
	
	public void reducerTask() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	{
		FileReader fileReader = new FileReader("/home/kaushikveluru/Desktop/Project/alice_map_output.txt");
		BufferedReader bufferedReader = new BufferedReader(fileReader);
		
		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = String.class;
		cArgs[1] = ArrayList.class;
		cArgs[2] = Context.class;
				
		Method reduceMethod = reducerCls.getMethod("reduce",cArgs);
		
		System.out.println("got Method reduce from Reducer class: "+reduceMethod.toString());
		Context reduceContext = new Context();
		reduceContext.setup("/home/kaushikveluru/Desktop/Project/alice_reduce_output.txt");
		
		HashMap<String, ArrayList<Integer>> wordMap = new HashMap<>();
		
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
		
	}
	
	public int waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{
		
		int status = -1;
		
		mapperTask();
		
		reducerTask();
		
		
		return status;
	}
}
