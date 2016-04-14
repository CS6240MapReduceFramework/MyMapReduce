package main;

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



public class WebClient {

    static String input_bucket = "";
    public static ArrayList<String> outputRecords = new ArrayList<String>();
    static private AWSCredentials credentials = new BasicAWSCredentials("***", "***");
    static private AmazonS3 s3Client = new AmazonS3Client(credentials);

    public static ArrayList<String> getFilesList()
    {
        ArrayList<String> files = new ArrayList<String>();
        try
        {

            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(input_bucket).withPrefix("climate/");
            ObjectListing objectListing;
            

            do {
                objectListing = s3Client.listObjects(listObjectsRequest);
                //System.out.println("size of climate folder: "+ objectListing.getObjectSummaries());

                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    if(!objectSummary.getKey().equals("climate/"))
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

    	if(args.length != 3)
        {
            System.out.println("Not enough arguments passed");
            System.exit(1);
        }

        String input_file_bucket = args[0];
        String output_file_bucket = args[1];
        String instances_file = args[2];

        String[] input_s3path=input_file_bucket.split("//")[1].split("/");
        input_bucket = input_s3path[0];
        String input_file = input_s3path[1];

        String[] output_s3path=output_file_bucket.split("//")[1].split("/");
        String output_bucket = output_s3path[0];
        String output_file = output_s3path[1];


    	Scanner sc = new Scanner(new File(instances_file));
        int instances_num = Integer.parseInt(sc.nextLine());
        System.out.println("Instances count: "+instances_num);

        ArrayList<String> files = getFilesList();    

        int chunk_size = files.size()/instances_num;
        int remaining_chunk_size = files.size() % instances_num;

        int index = 0;
        int count = 0;
        TextSocket[] connections=new TextSocket[instances_num];
        while(sc.hasNextLine())
        {   
            count++;
            String[] line = sc.nextLine().split(";");
            String instance_id = line[0];
            String instance_ip = line[1];

            System.out.println("Establishing connection to: "+instance_ip);
            TextSocket conn = new TextSocket(instance_ip, 3002);
            connections[count-1]=conn;
            conn.putln(input_bucket);
            conn.putln(input_file);
            conn.putln(output_bucket);
            conn.putln(output_file);
            conn.putln(count+"");
            int i;

            int jaffa = 0;
            for(i=index; i < index+chunk_size; i++)
            {
                conn.putln(files.get(i));            
                System.out.println("sending from client... "+files.get(i));
                jaffa++;
            }
            
            // send the reamaining files to last instance
            if(count == instances_num)
            {
                for(i=index; i < index+remaining_chunk_size; i++)
                {
                    conn.putln(files.get(i));            
                   // System.out.println(files.get(i));
                    jaffa++;
                }

            }

            index = i; 
            conn.putln("");//Marking the end

            System.out.println("End of an instance "+ jaffa);
        }
        System.out.println("Before for");
        for(int i=0;i<instances_num;i++)
        {
            System.out.println("Closed: "+ i);
            String hShake=connections[i].getln();
            System.out.println("AClosed: "+ i);
            connections[i].close();

        }  
        System.out.println("after for");   

        //Sorting at client side begin

        gatherOutputFromS3(output_bucket,output_file);
        sortRecords();
        writeOutput();            
    }


    public static void sortRecords() {

        Collections.sort(outputRecords,new Comparator<String>(){
                public int compare(String p1, String p2) {
                    Integer t1=Integer.parseInt(p1.split("\t")[0]);
                    Integer t2=Integer.parseInt(p2.split("\t")[0]);
                    return t2.compareTo(t1);
                }
            });
    }

    public static void writeOutput() throws IOException
    {
        File outFile = new File("topten.txt");
        FileWriter fstream = new FileWriter(outFile);
        BufferedWriter out = new BufferedWriter(fstream);

        for (int i=0;i<10;i++)
        {
            out.write(outputRecords.get(i)+"\n");
            out.flush();   
        }
    }

    private static void gatherOutputFromS3(String bucket,String prefix)
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
    }


}

