package wordcount;
import hadoop.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
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
	
	/*
	 * args[0] - input directory
	 * */
	public static void main(String args[])
	{
		Job job = null;
		try
		{
			Configuration conf = new Configuration();
			String propertiesFile = "/home/kaushikveluru/Documents/Git/MyMapReduce/Hadoop/src/wordcount/config.properties";
			
			conf.loadProperties(propertiesFile);
			
			job = Job.getInstance(conf,"Word Count");
			job.setJarByClass(WordCount.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountRedcuer.class);
			job.setNumReduceTasks(2);
			FileInputFormat.addInputPath(job, args[0]);
			FileOutputFormat.setOutputPath(job,args[1]);
			
			job.waitForCompletion(true);
			
			System.out.println(job.getJobname()+" job completed successfully!");
		}
		catch(Exception e)
		{
			System.out.println(job.getJobname()+" job failed!!");
			e.printStackTrace();
		}
	}
}
