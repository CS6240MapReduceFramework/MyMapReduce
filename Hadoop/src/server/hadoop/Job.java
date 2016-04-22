package hadoop;

import java.io.*;

import com.amazonaws.*;
import org.apache.commons.logging.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;

import textsock.TextSocket;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;


import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Properties;
import java.util.StringTokenizer;

public class Job {

	private String jobname;
	public Object mapperInstance;
	public Class<?> mapperCls;
	public Class<?> partitionerCls;
	public Object partitionerInstance;
	public Class<?> reducerCls;
	public Object reducerInstance;
	private Class jar;
	private static Job job;
	public int NUM_REDUCE_TASKS = 1;
	public int MAPPER_TASKS = 0;
	public int REDUCER_TASKS = 0;
	public double mapperPercentageComplete = 0.00d;
	public Configuration conf;
	public File[] partFiles;

	static Properties prop = new Properties();
	static {
		try {
			//Properties file should be within src folder
			prop.load(new FileInputStream("config.properties"));
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	static private AWSCredentials credentials = new BasicAWSCredentials(prop.getProperty("AWSAccessKeyId"), prop.getProperty("AWSSecretKey"));
	static private AmazonS3 s3Client = new AmazonS3Client(credentials);


	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	public static Job getInstance(Configuration conf,String jobname)
	{
		job = new Job();
		job.jobname = jobname;
		job.conf = conf;

		return job;
	}

	public void setMapperClass(Class<? extends Mapper> mapperClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		mapperCls = Class.forName(mapperClass.getName());
		mapperInstance = mapperCls.newInstance();
	}

	public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		partitionerCls = Class.forName(partitionerClass.getName());
		partitionerInstance = partitionerCls.newInstance();
	}


	public void setNumReduceTasks(int reduceTasks)
	{
		job.NUM_REDUCE_TASKS = reduceTasks;
	}
	public void setReducerClass(Class<? extends Reducer> reducerClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException
	{
		reducerCls = Class.forName(reducerClass.getName());
		reducerInstance = reducerCls.newInstance();

	}

	//TODO: Identify the purpose of this method
	public void setJarByClass(Class jarClass) throws ClassNotFoundException
	{
		ClassLoader classLoader = jarClass.getClassLoader();
		job.jar = classLoader.loadClass(jarClass.getName());
	}


	public void reducerTask(String outputBucket, String instanceIp) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	{

		getFileFromS3(outputBucket,instanceIp+"/output","reducelocal");

		//TODO: How to get data types for reduce method dynamically?
		Class[] cArgs = new Class[3];
		cArgs[0] = Text.class;
		cArgs[1] = Iterator.class;
		cArgs[2] = Context.class;
		Method reduceMethod = job.reducerCls.getMethod("reduce",cArgs);
		Context reduceContext = new Context();
		reduceContext.foldername = "output";

		HashMap<String,ArrayList<IntWritable>> wordMap = new HashMap<String,ArrayList<IntWritable>>();

		File tempFiles = new File("reducelocal/"+instanceIp+"/output");
		job.partFiles = tempFiles.listFiles();

		IntWritable one = new IntWritable(1);

		//TODO: This logic of writing into a HashMap should be changed
		for(int i=0; i<job.partFiles.length;i++)
		{
			FileReader fileReader = new FileReader(job.partFiles[i]);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			
			String line = null;
			while((line = bufferedReader.readLine())!= null)
			{
				String[] lines = line.split("\t");
				if(wordMap.containsKey(lines[0]))
				{
					ArrayList<IntWritable> list = wordMap.get(lines[0]);
					list.add(one);
					wordMap.put(lines[0],list);
				}
				else
				{
					ArrayList<IntWritable> list = new ArrayList<IntWritable>();
					list.add(one);
					wordMap.put(lines[0], list);
				}
			}
			bufferedReader.close();
		}

		//invoke reduce method for each key
		for(String key : wordMap.keySet())
		{
			Text text = new Text();
			text.set(key);
			
			Iterator<IntWritable> itr = wordMap.get(key).iterator();
			
			reduceMethod.invoke(job.reducerInstance,text,itr,reduceContext);
		}
	}


	//Upload a directory from local folder to given S3Folder
	public static void uploadToS3(String bucketName, String toS3Folder, String fromLocalFolder)
	{
		try
		{
			File from =new File(fromLocalFolder);
			System.out.println("Uploading files to S3 from a EC2 instance\n");

			TransferManager tx = new TransferManager(credentials);
			MultipleFileUpload mu =  tx.uploadDirectory(bucketName, toS3Folder,from,true);

			System.out.println("Waiting while uploading to S3");
			mu.waitForCompletion();
			System.out.println("Files uploading to S3 completed");
		}
		catch(AmazonServiceException ase)
		{
			ase.printStackTrace();
		}
		catch(AmazonClientException ace)
		{
			ace.printStackTrace();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}


	private static void getFileFromS3(String bucketName, String fromS3Folder, String toLocalFolder)
	{
		try
		{

			File localFolder = new File(toLocalFolder);
			if(!localFolder.exists())
				localFolder.mkdirs();

			TransferManager tx = new TransferManager(credentials);
			MultipleFileDownload md = tx.downloadDirectory(bucketName, fromS3Folder, localFolder);
			System.out.println("Waiting for files to download from S3");
			md.waitForCompletion();
			System.out.println("Files downloading from S3 to local folder completed");
		}
		catch(AmazonServiceException ase)
		{
			ase.printStackTrace();
		}
		catch(AmazonClientException ace)
		{
			ace.printStackTrace();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
	}

	public void mapperTask(String inputBucket, String instanceIp) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException, Exception
	{

		getFileFromS3(inputBucket,instanceIp,"inputlocal");


		String line = null;
		
		//TODO: How to get data types for reduce method dynamically?
		Class[] cArgs = new Class[3];
		cArgs[0] = Object.class;
		cArgs[1] = Text.class;
		cArgs[2] = Context.class;

		System.out.println("Mapper class instantiated..");
		Method mapMethod = job.mapperCls.getMethod("map",cArgs);

		Context mapContext = new Context();
		mapContext.foldername = instanceIp+"/tempFiles";


		File inputDir = new File("inputlocal/"+instanceIp+"/input");

		System.out.println("input file path: "+inputDir.getAbsolutePath());


		System.out.println("for each file in inputlocal/"+instanceIp+"/input:");
		for(File inputFile : inputDir.listFiles())
		{
			FileReader fileReader = new FileReader(inputFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			Text text = new Text();
			System.out.println("calling map method in mapper task");
			while((line = bufferedReader.readLine())!= null)
			{		
				text.set(line);
				mapMethod.invoke(job.mapperInstance,new Object(),text,mapContext);

			}
			bufferedReader.close();
		}
	}

	public void waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, Exception
	{

		System.out.println("start - waitForCompletion");
		int port = 3002;
		String ip = InetAddress.getLocalHost().getHostAddress();

		System.out.println("IPAddress of this ec2 instance: "+ip);
		TextSocket.Server svr = new TextSocket.Server(port);

		TextSocket conn;

		while (null != (conn = svr.accept())) {
			System.out.println("Server is listening....");

			String inputBucket = conn.getln();

			//the instance ip address sent from the client is the input folder for this instance
			String instanceIp = conn.getln();
			String outputBucket = conn.getln();

			String command = conn.getln();

			if(command.equals("MAPPER_START"))
				mapperTask(inputBucket,instanceIp);
			else
				System.out.println("Expected Command: MAPPER_START. Received command: "+command);


			System.out.println("Copying key files from Mapper to S3");
			//copy temp files after Mapper to S3
			uploadToS3(inputBucket, instanceIp+"/tempFiles",instanceIp+"/tempFiles");

			System.out.println("key files from Mapper uploaded to S3");

			conn.putln("MAPPER_COMPLETE");

			if(conn.getln().equals("REDUCER_START"))
			{
				reducerTask(inputBucket, instanceIp);
			}            

			uploadToS3(outputBucket, "output", "reducelocal/output");
			conn.putln("REDUCER_COMPLETE");

			System.out.println("Closing socket...");
			conn.close();
			svr.close();
			break;
		}    

	}
}
