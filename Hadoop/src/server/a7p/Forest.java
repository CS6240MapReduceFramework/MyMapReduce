package a7p;

import hadoop.*;

//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Forest {

	public static void main(String[] args) throws Exception {

		Job job = null;
		try
		{
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Random Forest");
			//job.setJarByClass(Forest.class);
			job.setMapperClass(FlightModelMapper.class);
			job.setReducerClass(FlightClassifyReducer.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FlightWritable.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			//FileInputFormat.addInputPath(job, new Path(args[0]));
			//FileOutputFormat.setOutputPath(job, new Path(args[1]));

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
