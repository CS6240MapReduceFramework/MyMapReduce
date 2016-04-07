package wordcount;
import hadoop.*;
import java.nio.file.Path;

public class WordCount {
	
	public static class WordCountMapper extends Mapper
	{
		public void map(Object key, Text value, Context context)
		{
			
		}
	}
	
	public static class WordCountRedcuer extends Reducer
	{	
		public void reduce(Text key, Iterable<Text> values, Context context)
		{
			
		}
		
	}
	
	public static void main(String args[])
	{
		try
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf,"Word Count");
			job.setJarByClass(WordCount.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountRedcuer.class);
			FileInputFormat.addInputPath(job, args[0]);
			FileOutputFormat.setOutputPath(job,args[1]);
			
			int status = job.waitForCompletion(true);
			if(status==-1)
				System.out.println(job.getJobname()+" job failed");
			else
				System.out.println(job.getJobname()+" job completed successfully");
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
