package wordcount;
import hadoop.*;

public class WordCount {
	
	public static class WordCountMapper extends Mapper
	{
		public void getKaushik()
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
			//job.setJarByClass(WordCount.class);
			job.setMapperClass(WordCountMapper.class,"wordcount.WordCount$WordCountMapper");
//			job.setReducerClass(WordCountRedcuer.class);
//			System.exit(job.waitForCompletion(true)? 0 :1);
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}
}
