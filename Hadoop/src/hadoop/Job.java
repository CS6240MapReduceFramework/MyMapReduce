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
	
	//public void setMapperClass(Class<? extends Mapper> mapperClass) throws ClassNotFoundException
	public void setMapperClass(Class<? extends Mapper> mapperClass, String mapperClassString) throws ClassNotFoundException
	{
		//TODO: get the mapperClassString from mapperClass
		job.mapper = mapperClass.getClassLoader().loadClass(mapperClassString);
		System.out.println(job.mapper.getCanonicalName());
		System.out.println("its working for now!");
		
		
	}
}
