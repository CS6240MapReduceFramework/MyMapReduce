package client;

import java.io.*;
import java.lang.*;
import textsock.TextSocket;
import java.util.*;

import javax.swing.plaf.SliderUI;

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
	static private AWSCredentials credentials = new BasicAWSCredentials("", "");
	static private AmazonS3 s3Client = new AmazonS3Client(credentials);
	static String inputBucket;

	public static ArrayList<String> getFilesList()
	{
		ArrayList<String> files = new ArrayList<String>();
		try
		{

			ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(inputBucket).withPrefix("input/");
			ObjectListing objectListing;


			do {
				objectListing = s3Client.listObjects(listObjectsRequest);
				//System.out.println("size of climate folder: "+ objectListing.getObjectSummaries());

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
	/* WebClient takes in 3 arguments: 

        args[0] = Input file bucket
        args[1] = Output file bucket
        args[2] = Instances.txt
	 */ 
	public static void main(String[] args) throws IOException {

		if(args.length != 4)
		{
			System.out.println("Not enough arguments passed");
			System.exit(1);
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
		Scanner sc = new Scanner(new File(instancesFile));
		int instances_num = Integer.parseInt(sc.nextLine());
		System.out.println("Instances count: "+instances_num);


		int count = 0;

		String instance_ip="";
		TextSocket[] connections=new TextSocket[instances_num];
		while(sc.hasNextLine())
		{   
			String[] line = sc.nextLine().split(";");
			String instance_id = line[0];
			instance_ip = line[1];

			System.out.println("Establishing connection to: "+instance_ip);
			TextSocket conn = new TextSocket(instance_ip, 3002);
			connections[count]=conn;

			/*			try
			{
				s3Client.putObject(new PutObjectRequest(inputBucket, inputFolderInBucket, instanceFolder));
			}
			catch (AmazonServiceException ase) {
	            System.out.println("Error Message:    " + ase.getMessage());
	        }
			catch (AmazonClientException ace) {
	            System.out.println("Error Message: " + ace.getMessage());
	        }
			 */			

			//Send application program name
			conn.putln(programName);

			//send corresponding folder in s3 for this instance
			conn.putln(inputBucket);

			//send instance ip
			conn.putln(instance_ip);

			//Send the same output folder for all instances
			conn.putln(outputBucket);


			//conn.putln("");//Marking the end

			System.out.println("Program started on"+instance_ip);
		}


		//Divide files before starting mapper

		ArrayList<String> files = getFilesList();    

		int chunk_size = files.size()/instances_num;
		int remaining_chunk_size = files.size() % instances_num;


		int index = 0;
		
		for(int i=0;i<files.size();i++)
		{
			try {
				System.out.println("File being copied from : "+files.get(i)+" to :"+instance_ip+"/"+files.get(i));
	            // Copying object
	            CopyObjectRequest copyObjRequest = new CopyObjectRequest(
	            		inputBucket, files.get(i), inputBucket, instance_ip+"/"+files.get(i));
	            System.out.println("Copying object.");
	            s3Client.copyObject(copyObjRequest);
	            System.out.println("copied object");
	            
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


		for(int i=0;i<instances_num;i++)
		{
			connections[i].putln("MAPPER_START");
		}


		//Wait for mapper completion on all instances
		for(int i=0;i<instances_num;i++)
		{
			connections[i].getln();
			System.out.println("MAPPER COMPLETED ON: "+ i);
		}        

		//Copy and merge temp files to S3


		//Start reducer
		for(int i=0;i<instances_num;i++)
		{
			connections[i].putln("REDUCER_START");
			System.out.println("REDUCER STARTED ON: "+ i);
		}      



		for(int i=0;i<instances_num;i++)
		{
			connections[i].getln();
			System.out.println("REDUCER COMPLETED ON : "+ i);
			connections[i].close();

		}  

		// gatherOutputFromS3(output_bucket,output_file);
	}


	/*private static void gatherOutputFromS3(String bucket,String prefix)
	{
		S3Object s3object = new S3Object();
		try
		{
			s3object = s3Client.getObject(new GetObjectRequest(bucket, prefix));
			InputStream is = s3object.getObjectContent();
			BufferedReader br = new BufferedReader(new InputStreamReader(is));
			String line = null;
			while ((line = br.readLine()) != null) {
				outputRecords.add(line);
			}
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
	}*/


}

