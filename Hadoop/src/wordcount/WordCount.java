package wordcount;
import hadoop.*;

public class WordCount {
	
	public static class WordCountMapper extends Mapper
	{
		public static void getKaushik()
		{
			System.out.println("Inside getKaushik method in WordCountMapper class");
		}
	}
	
	public static class WordCountRedcuer extends Reducer
	{
		
	}
	
	public static void main(String args[])
	{
		try
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf,"Word Count");
			job.setJarByClass(WordCount.class,"wordcount.WordCount");
			job.setMapperClass(WordCountMapper.class,"wordcount.WordCount$WordCountMapper");
			job.setReducerClass(WordCountRedcuer.class,"wordcount.WordCount$WordCountRedcuer");
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
