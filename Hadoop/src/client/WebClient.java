package client;

import java.io.*;
import java.lang.*;
import textsock.TextSocket;
import java.util.*;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;




public class WebClient {

	public static ArrayList<String> outputRecords = new ArrayList<String>();
	
	static Properties prop = new Properties();
	static {
		try {
			//Properties file should be within src folder
			prop.load(new FileInputStream("../config.properties"));
		}catch(Exception e){
			System.out.println(e.getMessage());
		}
	}
	static private AWSCredentials credentials = new BasicAWSCredentials(prop.getProperty("AWSAccessKeyId"), prop.getProperty("AWSSecretKey"));
	static private AmazonS3 s3Client = new AmazonS3Client(credentials);
	static String inputBucket;


	/*
	 * Fetches the list of file names in s3://<inputBucket>/input folder
	 * Input Arguments: nil
	 * Returns: ArrayList<String>  - List of file names in the given s3 folder
	 */
	public static ArrayList<String> getFilesList()
	{
		ArrayList<String> files = new ArrayList<String>();
		try
		{
			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(inputBucket).withPrefix("input/");
			ObjectListing objectListing;

			do {
				objectListing = s3Client.listObjects(listObjectsRequest);

				for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
					if(!objectSummary.getKey().equals("input/"))
						files.add(objectSummary.getKey());
				}

				listObjectsRequest.setMarker(objectListing.getNextMarker());
			} while (objectListing.isTruncated());

		}
		catch(AmazonServiceException ase)
		{
			System.out.println( "AmazonServiceException" );
			ase.printStackTrace();
		}
		catch(AmazonClientException ace)
		{
			System.out.println( "AmazonClientException" );
		}
		finally
		{
			return files;
		}

	}


	public static void divideFilesInS3(int instancesCount,String[] ips)
	{
		ArrayList<String> files = getFilesList();    

		int chunk_size = files.size()/instancesCount;
		int remaining_chunk_size = files.size() % instancesCount;


		int instance = 0;

		for(int i=0;i<files.size();i++)
		{
			try {

				if(instance!=instancesCount)
					instance = i/chunk_size;

				System.out.println("File being copied from S3 to ec2 instance");
				// Copying object
				CopyObjectRequest copyObjRequest = new CopyObjectRequest(
						inputBucket, files.get(i), inputBucket, ips[instance]+"/"+files.get(i));
				System.out.println("Copying object.");
				s3Client.copyObject(copyObjRequest);
				System.out.println("copied object");

				//TODO: Replace sleep with waitForCompletion method in copyObject
				System.out.println("sleeping for 10000 ms");

				Thread.sleep(10000);


			} catch (AmazonServiceException ase) {

				System.out.println("Error Message:    " + ase.getMessage());

			} catch (AmazonClientException ace) {

				System.out.println("Error Message: " + ace.getMessage());
			}catch (InterruptedException ioe)
			{
				System.out.println("error message in threadl.sleep");
			}
		}

	}

	public static void startMappersPhase(TextSocket[] connections) throws IOException
	{
		for(TextSocket connection : connections)
			connection.putln("MAPPER_START");
	}

	public static void startReducersPhase(TextSocket[] connections) throws IOException
	{
		System.out.println("sending REDUCER_START signal to all instances");
		for(TextSocket connection : connections)
			connection.putln("REDUCER_START");
	}

	public static void waitForMappersPhaseCompletion(TextSocket[] connections) throws IOException
	{
		for(TextSocket connection : connections)
			connection.getln();
	}

	public static void closeConnections(TextSocket[] connections) throws IOException
	{
		System.out.println("REDUER_COMPLETE signal receieved. Closing all connections");
		for(TextSocket connection : connections)
		{
			connection.getln();
			connection.close();
		}
		System.out.println("All connections closed successfully!");

	}
	
	public static void downloadOutputPartFilesFromS3(String[] ips,String outputBucket)
	{
		try
		{

			//TODO: Remove hard-coding of the output folder
			File localFolder = new File("FinalOutput");
			if(!localFolder.exists())
				localFolder.mkdirs();

			for(int i=0;i<ips.length; i++)
			{
				TransferManager tx = new TransferManager(credentials);
				tx.downloadDirectory(outputBucket, "output", localFolder);
				//TODO: Add waitForCompletion logic here
				System.out.println("sleeping while downloading all part output files");
				Thread.sleep(5000);
			}
			
			System.out.println("All output part files from S3://<outputBucket>/output downloaded");

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

	public static void downloadIntermediateFilesFromS3(String[] ips)
	{
		try
		{

			File localFolder = new File("allTempFiles");
			if(!localFolder.exists())
				localFolder.mkdirs();

			for(int i=0;i<ips.length; i++)
			{
				TransferManager tx = new TransferManager(credentials);
				tx.downloadDirectory(inputBucket, ips[i]+"/tempFiles", localFolder);
				//TODO: Add waitForCompletion logic here
				System.out.println("sleeping while downloading all temp files");
				Thread.sleep(5000);
			}
			
			System.out.println("All temp files from all instances downloaded");


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

	/* WebClient takes in 3 arguments: 

        args[0] = Input file bucket   	Ex: s3://<inputBucket>/input
        args[1] = Output file bucket	Ex: s3://<outputBucket>/output
        args[2] = Instances.txt		- 	A file with list of instances created by start-cluster.sh and their details
        args[3] = Program name		- 	Should match with the application name in the server. Ex: WordCount. Not Word Count
	 */ 
	public static void main(String[] args) throws IOException {

		if(args.length != 4)
		{

			System.out.println("Not enough arguments passed");
			System.exit(1);
		}
		else
		{
			//TODO: displayCorrectUsage();
			//TODO: exit at this point
		}

		String inputDataLocation = args[0];
		String outputDataLocation = args[1];
		String instancesFile = args[2];
		String programName = args[3];

		String[] inputDataLocationSplits=inputDataLocation.split("//")[1].split("/");
		inputBucket = inputDataLocationSplits[0];
		String inputFolderInBucket = inputDataLocationSplits[1];

		String[] outputDataLocatonSplits=outputDataLocation.split("//")[1].split("/");
		String outputBucket = outputDataLocatonSplits[0];
		String outputFolderInBucket = outputDataLocatonSplits[1];


		//Read the instances.txt file 
		System.out.println("Reading instances.txt file");
		Scanner sc = new Scanner(new File(instancesFile));
		int instances_num = Integer.parseInt(sc.nextLine());
		System.out.println("Instances count: "+instances_num);


		int count = 0;

		String ips[] = new String[instances_num];
		String instanceIp="";
		TextSocket[] connections=new TextSocket[instances_num];

		//TODO: Convert this sequential code to parallel using Threads
		while(sc.hasNextLine())
		{   
			String[] line = sc.nextLine().split(";");
			String instance_id = line[0];
			instanceIp = line[1];
			ips[count] = instanceIp;

			System.out.println("Establishing connection to: "+instanceIp);
			TextSocket conn = new TextSocket(instanceIp, 3002);

			System.out.println("Connection established..");
			connections[count]=conn;

			count++;

			//send corresponding folder in s3 for this instance
			conn.putln(inputBucket);

			//send instance ip
			conn.putln(instanceIp);

			//Send the same output folder for all instances
			conn.putln(outputBucket);

			System.out.println("Program started on"+instanceIp);
		}

		divideFilesInS3(instances_num, ips);
		startMappersPhase(connections);
		waitForMappersPhaseCompletion(connections);

		//TODO: Downloading files completed. TODO merge the common files
		downloadIntermediateFilesFromS3(ips);
		//TODO: divide merged files equally for all instances and push to S3 accordingly 

		startReducersPhase(connections);
		closeConnections(connections);
		
		downloadOutputPartFilesFromS3(ips,outputBucket);

	}

}

