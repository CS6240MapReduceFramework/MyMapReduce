package hadoop;

public class Job {

	private String jobname;
	private Class<?> mapper;
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
		classLoader.loadClass(name)
		job.mapper = classLoader.loadClass("wordcount.WordCountMapper");
		
		
	}
}
