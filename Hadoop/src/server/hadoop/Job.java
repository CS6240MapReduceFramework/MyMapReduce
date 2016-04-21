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

	public void setJarByClass(Class jarClass) throws ClassNotFoundException
	{
		ClassLoader classLoader = jarClass.getClassLoader();
		job.jar = classLoader.loadClass(jarClass.getName());
	}


	 	public void reducerTask(String inputBucket,String instanceIp) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	 	{


	 		Class[] cArgs = new Class[3];
	 		cArgs[0] = String.class;
	 		cArgs[1] = ArrayList.class;
	 		cArgs[2] = Context.class;
	 		Method reduceMethod = job.reducerCls.getMethod("reduce",cArgs);
	 		Context reduceContext = new Context();
	 		reduceContext.foldername = instanceIp+"/output";

	 		HashMap<String,ArrayList<Integer>> wordMap = new HashMap<String,ArrayList<Integer>>();
	 		
	 		File tempFiles = new File(instanceIp+"/tempFiles");
	 		job.partFiles = tempFiles.listFiles();

	 		
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
	 					ArrayList<Integer> list = wordMap.get(lines[0]);
	 					list.add(1);
	 					wordMap.put(lines[0],list);
	 				}
	 				else
	 				{
	 					ArrayList<Integer> list = new ArrayList<Integer>();
	 					list.add(1);
	 					wordMap.put(lines[0], list);
	 				}
	 			}

	 			bufferedReader.close();

			}


	 		for(String key : wordMap.keySet())
	 		{
	 			reduceMethod.invoke(job.reducerInstance,key,wordMap.get(key),reduceContext);
	 		}
	 	}

	 	
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
//			Thread.sleep(20000);
			System.out.println("Temp files uploaded ");

		}
		catch(AmazonServiceException ase)
		{
			System.out.println( "AmazonServiceException" );
			ase.printStackTrace();
		}
		catch(AmazonClientException ace)
		{
			System.out.println( "AmazonClientException" );
			ace.printStackTrace();
		}
		catch(Exception e)
		{
			System.out.println("Excepton");
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
			//waitForCompletion for download to complete
			md.waitForCompletion();


		}
		catch(AmazonServiceException ase)
		{
			System.out.println( "AmazonServiceException" );
			ase.printStackTrace();
		}
		catch(AmazonClientException ace)
		{
			System.out.println( "AmazonClientException" );
			ace.printStackTrace();
		}
		catch(Exception e)
		{
			System.out.println("Excepton");
			e.printStackTrace();
		}
	}
	public void mapperTask(String inputBucket, String instanceIp) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException, Exception
	{

		getFileFromS3(inputBucket,instanceIp,"inputlocal");

		System.out.println("Files downloaded to local from S3 to location: inputlocal/"+instanceIp+"/input");

		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = Object.class;
		cArgs[1] = String.class;
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

			System.out.println("calling map method in mapper task");
			while((line = bufferedReader.readLine())!= null)
			{		
				mapMethod.invoke(job.mapperInstance,new Object(),line,mapContext);

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

			String programName = conn.getln();
			//System.out.println("program name :"+ programName);

			String inputBucket = conn.getln();
			//System.out.println("Input bucket : "+ inputBucket);

			//the instance ip address sent from the client is the input folder for this instance
			String instanceIp = conn.getln();
			//System.out.println("input folder (ip address): "+ inputFolder);


			String outputBucket = conn.getln();
			//System.out.println("output bucket: "+outputBucket);


			String command = conn.getln();
			if(command.equals("MAPPER_START"))
				mapperTask(inputBucket,instanceIp);
			else
				System.out.println("Expected Command: MAPPER_START. Received command: "+command);


			System.out.println("Key files from mappers created..");
			//copy temp files after Mapper to S3
			uploadToS3(inputBucket, instanceIp+"/tempFiles",instanceIp+"/tempFiles");
			
			
			conn.putln("MAPPER_COMPLETE");


			if(conn.getln().equals("REDUCER_START"))
			{
				reducerTask(inputBucket, instanceIp);
			}            

			uploadToS3(outputBucket, instanceIp+"/output", instanceIp+"/output");
			conn.putln("REDUCER_COMPLETE");

			System.out.println("Closing socket...");
			conn.close();
			svr.close();
			break;
		}    

	}
}
