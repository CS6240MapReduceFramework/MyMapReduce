package wordcount;
import hadoop.*;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class WordCount {
	
	public static class WordCountMapper extends Mapper
	{
		private String word = new String();
		private Integer count = new Integer(1);
		
		public void map(Object key, String value, Context context) throws IOException
		{
			
			
			StringTokenizer tokens = new StringTokenizer(value, " ");
			while(tokens.hasMoreTokens())
			{
				word = tokens.nextToken();
				context.write(word,count);
			}
		}
	}
	
	public static class WordCountRedcuer extends Reducer
	{	
		public void reduce(String key, ArrayList<Integer> values, Context context) throws IOException
		{
			int sum = 0;
		      for (Integer val : values) {
		        sum += val;
		      }
		      
		      context.write(key,sum);
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
