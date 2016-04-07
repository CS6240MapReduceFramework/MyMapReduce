package hadoop;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class Job {

	private String jobname;
	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	private Class<?> mapper;
	private Class<?> reducer;
	private Class jar;
	private static Job job;
	
	public static Job getInstance(Configuration conf,String jobname)
	{
		job = new Job();
		job.jobname = jobname;
		
		return job;
	}
	
	public void setMapperClass(Class<? extends Mapper> mapperClass) throws ClassNotFoundException
	{
		ClassLoader classLoader = mapperClass.getClassLoader();
		job.mapper = classLoader.loadClass(mapperClass.getName());
//		Method method = job.mapper.getMethod("map");
//		method.invoke(null);	
	}
	
	public void setReducerClass(Class<? extends Reducer> reducerClass) throws ClassNotFoundException
	{
		ClassLoader classLoader = reducerClass.getClassLoader();
		job.reducer = classLoader.loadClass(reducerClass.getName());
	}
	
	public void setJarByClass(Class jarClass) throws ClassNotFoundException
	{
		ClassLoader classLoader = jarClass.getClassLoader();
		job.jar = classLoader.loadClass(jarClass.getName());
	}
	
	public int waitForCompletion(Boolean bool)
	{
		
		int status = -1;
		return status;
	}
}
