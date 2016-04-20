package hadoop;

public class FileOutputFormat {

	public static void setOutputPath(Job job,String outputDir)
	{
		job.conf.prop.setProperty("OUTPUT_DIR", outputDir);
	}
}
