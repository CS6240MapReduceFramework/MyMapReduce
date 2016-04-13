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
	public Class<?> partitionerCls;
	public Object partitionerInstance;
	public Class<?> reducerCls;
	public Object reducerInstance;
	private Class jar;
	private static Job job;
	public int NUM_REDUCE_TASKS;
	public int MAPPER_TASKS = 0;
	public int REDUCER_TASKS = 0;
	public double mapperPercentageComplete = 0.00d;
	public Configuration conf;
	public File[] partFiles;

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

	public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		partitionerCls = Class.forName(partitionerClass.getName());
		partitionerInstance = partitionerCls.newInstance();
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
	

	public boolean isMapPhaseComplete(ArrayList<MapperThread> threads)
	{
		boolean bool = true;
		int count = threads.size();
		float completeCount = 0.0f;
		double percentageCompleted = 0.00d;
		for(MapperThread t : threads)
		{	
			if(t.status.equals("COMPLETED"))
				completeCount++;

			bool = bool && t.status.equals("COMPLETED");
		}


		percentageCompleted = Math.round((completeCount/count)*100.0);
		if(job.mapperPercentageComplete != percentageCompleted)
		{
			job.mapperPercentageComplete = percentageCompleted;
			System.out.println("Map progress : "+ percentageCompleted+"% completed.");
		}
		
		return bool;
	}
	
	public boolean isReducePhaseComplete(ArrayList<ReducerThread> threads)
	{
		boolean bool = true;
		int count = threads.size();
		float completeCount = 0.0f;
		double percentageCompleted = 0.00d;
		for(ReducerThread t : threads)
		{	
			if(t.status.equals("COMPLETED"))
				completeCount++;

			bool = bool && t.status.equals("COMPLETED");
		}


		percentageCompleted = Math.round((completeCount/count)*100.0);
		if(job.mapperPercentageComplete != percentageCompleted)
		{
			job.mapperPercentageComplete = percentageCompleted;
			System.out.println("Reduce progress : "+ percentageCompleted+"% completed.");
		}
		
		return bool;
	}

	public void waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException
	{

		//Cleans TEMP_DIR and OUTPUT_DIR
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
		File temp_folder = new File(job.conf.prop.getProperty("TEMP_DIR"));
		int part_files_count = temp_folder.listFiles().length;
		partFiles = temp_folder.listFiles();
		
		int files_per_reducer = part_files_count / job.NUM_REDUCE_TASKS;
		int remaining_files = part_files_count % job.NUM_REDUCE_TASKS;
		ArrayList<ReducerThread> reducerThreadsList = new ArrayList<ReducerThread>();
		
		for(int i=0;i< job.NUM_REDUCE_TASKS;i++)
		{
	
			ReducerThread reducer = new ReducerThread(job);
			
			reducer.no_of_files = part_files_count;
			reducer.startIndex = i*files_per_reducer;
			if(i==job.NUM_REDUCE_TASKS)
			{
				reducer.endIndex = reducer.startIndex + remaining_files - 1;
			}
			else
			{
				reducer.endIndex = reducer.startIndex + files_per_reducer - 1;
			}
		
			
			reducerThreadsList.add(reducer);
			
			Thread t = new Thread(reducer,""+i);
			t.start();
		}

		while(true)
		{
			if(isReducePhaseComplete(reducerThreadsList))
			{
				break;
			}
			else
			{
				//System.out.println("Reducer not completed yet");
			}
		}


	}
}
