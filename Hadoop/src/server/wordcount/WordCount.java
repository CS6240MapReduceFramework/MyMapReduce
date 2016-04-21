package wordcount;
import hadoop.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.StringTokenizer;

public class WordCount {

	//TODO: Add template to Mapper. Example : Mapper<KEYIN, KEYOUT>
	public static class WordCountMapper extends Mapper
	{	
		//TODO: Use Text and IntWritable 
		private String word = new String();
		private Integer count = new Integer(1);

		public void map(Object key, String value, Context context) throws IOException
		{

			//System.out.println("inside map method...");
			String input = value.replaceAll("[^a-zA-Z0-9]","");

			StringTokenizer tokens = new StringTokenizer(value, " ");
			while(tokens.hasMoreTokens())
			{
				word = tokens.nextToken().trim();
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
		Job job = null;
		try
		{
			Configuration conf = new Configuration();

			job = Job.getInstance(conf,"Word Count");

			job.setJarByClass(WordCount.class);
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountRedcuer.class);
			job.setNumReduceTasks(1);
			// FileInputFormat.addInputPath(job, args[0]);
			// FileOutputFormat.setOutputPath(job,args[1]);

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
