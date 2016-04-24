package wordmedian;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;


import hadoop.*;

public class WordMedian{

	private static double median = 0;
	private final static int ONE = 1;

	/**
	 * Maps words from line of text into a key-value pair; the length of the word
	 * as the key, and 1 as the value.
	 */
	public static class WordMedianMapper extends Mapper<Object,Text,IntWritable,IntWritable>{

		private IntWritable length = new IntWritable();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String string = itr.nextToken();
				length.set(string.length());
				context.write(length,ONE);
			}
		}
	}

	/**
	 * Performs integer summation of all the values for each key.
	 */
	public static class WordMedianReducer extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

		private IntWritable val = new IntWritable(0);

		public void reduce(IntWritable key, CustomIterable values, Context context) throws IOException, InterruptedException, Exception {

			
			while(values.hasNext())
			{
				val.set(val.get() + values.next().get());
			}
			context.write(key, val);
		}
	}

	/**
	 * This is a standard program to read and find a median value based on a file
	 * of word counts such as: 1 456, 2 132, 3 56... Where the first values are
	 * the word lengths and the following values are the number of times that
	 * words of that length appear.
	 * 
	 * @param path
	 *          The path to read the HDFS file from (part-r-00000...00001...etc).
	 * @param medianIndex1
	 *          The first length value to look for.
	 * @param medianIndex2
	 *          The second length value to look for (will be the same as the first
	 *          if there are an even number of words total).
	 * @throws IOException
	 *           If file cannot be found, we throw an exception.
	 * */
	private static double readAndFindMedian(String path, int medianIndex1, int medianIndex2, Configuration conf) throws IOException {

		File outputFile = new File("output");

		if(!outputFile.exists())
			throw new IOException("Output not found!");

		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader(outputFile));
			int num = 0;

			String line;
			while ((line = br.readLine()) != null) {
				StringTokenizer st = new StringTokenizer(line);

				// grab length
				String currLen = st.nextToken();

				// grab count
				String lengthFreq = st.nextToken();

				int prevNum = num;
				num += Integer.parseInt(lengthFreq);

				if (medianIndex2 >= prevNum && medianIndex1 <= num) {
					System.out.println("The median is: " + currLen);
					br.close();
					return Double.parseDouble(currLen);
				} else if (medianIndex2 >= prevNum && medianIndex1 < num) {
					String nextCurrLen = st.nextToken();
					double theMedian = (Integer.parseInt(currLen) + Integer
							.parseInt(nextCurrLen)) / 2.0;
					System.out.println("The median is: " + theMedian);
					br.close();
					return theMedian;
				}
			}
		} finally {
			if (br != null) {
				br.close();
			}
		}
		// error, no median found
		return -1;
	}

	public static void main(String[] args) {

		try
		{
//			if (args.length != 2) {
//				System.err.println("Usage: wordmedian <in> <out>");
//			}
			Configuration conf = new Configuration();

			Job job = Job.getInstance(conf,"Word Count");

			job.setMapperClass(WordMedianMapper.class);
			job.setReducerClass(WordMedianReducer.class);
			job.setOutputKeyClass(IntWritable.class);
			job.setOutputValueClass(IntWritable.class);
			//FileInputFormat.addInputPath(job, new Path(args[0]));
			//FileOutputFormat.setOutputPath(job, new Path(args[1]));
			job.waitForCompletion(true);

			//TODO: Counters
			long totalWords = 1000;
			int medianIndex1 = (int) Math.ceil((totalWords / 2.0));
			int medianIndex2 = (int) Math.floor((totalWords / 2.0));
			median = readAndFindMedian(args[1], medianIndex1, medianIndex2, conf);
			System.out.println("Median = "+median);

		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public double getMedian() {
		return median;
	}
}