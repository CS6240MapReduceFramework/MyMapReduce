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
	public void reducerTask(File tempFolder) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	{

		
		System.out.println("Reducer started");

		Class[] cArgs = new Class[3];
		cArgs[0] = String.class;
		cArgs[1] = ArrayList.class;
		cArgs[2] = Context.class;
		Method reduceMethod = job.reducerCls.getMethod("reduce",cArgs);
		Context reduceContext = new Context();
		String part_output_file = job.conf.prop.getProperty("OUTPUT_DIR")+"part-r-00000";
		reduceContext.setup(part_output_file);
		HashMap<String, ArrayList<Integer>> wordMap = new HashMap<>();
		
		for(File file : tempFolder.listFiles())
		{
			FileReader fileReader = new FileReader(file);
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

		System.out.println("Reduce : 100% complete");
	}

	public boolean isMapPhaseComplete(ArrayList<MapperThread> threads)
	{
		boolean bool = true;
		for(MapperThread t : threads)
		{
			bool = bool && t.status.equals("COMPLETED");
		}
		return bool;
	}

	public int waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{

		int status = -1;

		cleanUpFiles();


		File files = new File(job.conf.prop.getProperty("INPUT_DIR"));
		int mapperCount =0;

		ArrayList<MapperThread> threadsList = new ArrayList<MapperThread>();
		for(File file :files.listFiles())
		{
			mapperCount++;
			MapperThread runnable = new MapperThread(job,file);
			Thread thread = new Thread(runnable,"map-"+mapperCount);
			threadsList.add(runnable);
			thread.start();
		}

		while(true)
		{
			if(isMapPhaseComplete(threadsList))
			{
				break;
			}
			else
			{
				//System.out.println("Mappers not completed yet");
			}
		}
		System.out.println("Map : 100% complete");
		File temp_folder = new File(job.conf.prop.getProperty("TEMP_DIR"));
		
		reducerTask(temp_folder);
				



		return status;
	}
}
