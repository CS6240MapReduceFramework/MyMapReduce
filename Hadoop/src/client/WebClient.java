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
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.MultipleFileUpload;

public class WebClient {

    static Properties prop = new Properties();

    static {
        try {
            //Properties file should be within src folder
            prop.load(new FileInputStream("../config.properties"));
        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }

    public static ArrayList<String> outputRecords = new ArrayList<String>();
    static private AWSCredentials credentials = new BasicAWSCredentials(prop.getProperty("AWSAccessKeyId"), prop.getProperty("AWSSecretKey"));
    static private AmazonS3 s3Client = new AmazonS3Client(credentials);
    static String inputBucket;

    /*
     * Fetches the list of file names in s3://<inputBucket>/input folder
     * Input Arguments: nil
     * Returns: ArrayList<String>  - List of file names in the given s3 folder
     */
    public static ArrayList<String> getFilesList(String bucketfolder) {
        ArrayList<String> files = new ArrayList<String>();
        try {
            ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(inputBucket).withPrefix(bucketfolder + "/");
            ObjectListing objectListing;

            do {
                objectListing = s3Client.listObjects(listObjectsRequest);

                for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
                    if (!objectSummary.getKey().equals(bucketfolder + "/"))
                        files.add(objectSummary.getKey());
                }

                listObjectsRequest.setMarker(objectListing.getNextMarker());
            } while (objectListing.isTruncated());

        } catch (AmazonServiceException ase) {
            System.out.println("AmazonServiceException");
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            System.out.println("AmazonClientException");
        } finally {
            return files;
        }

    }


    public static void divideFilesInS3(int instancesCount, String[] ips, String bucketfolder) {
        ArrayList<String> files = getFilesList(bucketfolder);

        int chunk_size = files.size() / instancesCount;
        int remaining_chunk_size = files.size() % instancesCount;

        System.out.println("Number of files - "+files.size());
        int instance = 0;

        for (int i = 0; i < files.size(); i++) {
            try {

                if (instance != instancesCount)
                    instance = i / chunk_size;

                System.out.println("File being copied from S3 to ec2 instance - "+files.get(i));
                // Copying object
                CopyObjectRequest copyObjRequest = new CopyObjectRequest(
                        inputBucket, files.get(i), inputBucket, ips[instance] + "/" + files.get(i));

                System.out.println("Copying object.");
                TransferManager tx = new TransferManager(credentials);
                Copy cp = tx.copy(copyObjRequest);
                cp.waitForCompletion();
                System.out.println("copied object");

            } catch (AmazonServiceException ase) {

                System.out.println("Error Message:    " + ase.getMessage());

            } catch (AmazonClientException ace) {

                System.out.println("Error Message: " + ace.getMessage());
            } catch (InterruptedException ioe) {
                System.out.println("error message in threadl.sleep");
            }
        }

    }

    public static void startMappersPhase(TextSocket[] connections) throws IOException {
        for (TextSocket connection : connections)
            connection.putln("MAPPER_START");
    }

    public static void startReducersPhase(TextSocket[] connections) throws IOException {
        System.out.println("sending REDUCER_START signal to all instances");
        for (TextSocket connection : connections)
            connection.putln("REDUCER_START");
    }

    public static void waitForMappersPhaseCompletion(TextSocket[] connections) throws IOException {
        for (TextSocket connection : connections)
            connection.getln();
    }

    public static void closeConnections(TextSocket[] connections) throws IOException {
        System.out.println("REDUER_COMPLETE signal receieved. Closing all connections");
        for (TextSocket connection : connections) {
            connection.getln();
            connection.close();
        }
        System.out.println("All connections closed successfully!");

    }

    public static void downloadOutputPartFilesFromS3(String[] ips, String outputBucket) {
        try {

            //TODO: Remove hard-coding of the output folder
            File localFolder = new File("FinalOutput");
            if (!localFolder.exists())
                localFolder.mkdirs();

            for (int i = 0; i < ips.length; i++) {
                TransferManager tx = new TransferManager(credentials);
                MultipleFileDownload md = tx.downloadDirectory(outputBucket, "output", localFolder);
                md.waitForCompletion();
            }

            System.out.println("All output part files from S3://<outputBucket>/output downloaded");

        } catch (AmazonServiceException ase) {
            System.out.println("AmazonServiceException");
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            System.out.println("AmazonClientException");
            ace.printStackTrace();
        } catch (Exception e) {
            System.out.println("Excepton");
            e.printStackTrace();
        }

    }

    public static void downloadIntermediateFilesFromS3(String[] ips) {
        try {

            File localFolder = new File("allTempFiles");
            if (!localFolder.exists())
                localFolder.mkdirs();

            for (int i = 0; i < ips.length; i++) {
                TransferManager tx = new TransferManager(credentials);
                MultipleFileDownload md = tx.downloadDirectory(inputBucket, ips[i] + "/tempFiles", localFolder);
                md.waitForCompletion();
            }

            System.out.println("All temp files from all instances downloaded");


        } catch (AmazonServiceException ase) {
            System.out.println("AmazonServiceException");
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            System.out.println("AmazonClientException");
            ace.printStackTrace();
        } catch (Exception e) {
            System.out.println("Excepton");
            e.printStackTrace();
        }
    }

    public static void uploadFilesToS3(String localfolder, String bucketName, String bucketFolder) {

        try {
            File local = new File(localfolder);
            System.out.println("Uploading files to S3 from a local folder\n");

            TransferManager tx = new TransferManager(credentials);
            MultipleFileUpload mu = tx.uploadDirectory(bucketName, bucketFolder, local, true);
            mu.waitForCompletion();
            s3Client.deleteObject(new DeleteObjectRequest(bucketName, "output/.DS_Store"));

        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            ace.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void mergeIntermediateFilesFromS3() throws IOException {

        File tempfiles = new File("allTempFiles");

        if (!tempfiles.exists())
            System.out.println("Folder not found");
        FileWriter fileWriter;
        BufferedWriter bufferedWriter;
        //Open all instance folders in allTempFiles
        for (File instance : tempfiles.listFiles()) {

            if(instance.getName().contains("DS_Store")){
                continue;
            }

            System.out.println("Within folder - " + instance);

            //Open each instance folder
            File subFolder = new File(instance.getAbsolutePath() + "/tempFiles");

            //Iterate over all tempfiles in each isntance folder
            for (File tempFile : subFolder.listFiles()) {

                if(instance.getName().contains("DS_Store")){
                    continue;
                }
                //Open tempfile for read
                FileReader tmpFileReader = new FileReader(tempFile);
                BufferedReader bufferedReader = new BufferedReader(tmpFileReader);
                String line = "";

                //Open file in parent folder for writing the tempfile
                File fdir = new File("allTempFiles/merged");
                if (!fdir.exists())
                    fdir.mkdirs();

                File f = new File("allTempFiles/merged/" + tempFile.getName());

                if (!f.exists())
                    f.createNewFile();

                //create writer to copy all the content from tempfile to one tempfile in parent folder(true as second property)
                fileWriter = new FileWriter(f, true);
                bufferedWriter = new BufferedWriter(fileWriter);

                //read lines from temp instance file
                while ((line = bufferedReader.readLine()) != null) {

                    //write to one file
                    bufferedWriter.write(line + "\n");
                    bufferedWriter.flush();
                }

                //close writer
                bufferedWriter.close();
                //close reader
                bufferedReader.close();
            }

        }
    }

    /* WebClient takes in 3 arguments:

        args[0] = Input file bucket   	Ex: s3://<inputBucket>/input
        args[1] = Output file bucket	Ex: s3://<outputBucket>/output
        args[2] = Instances.txt		- 	A file with list of instances created by start-cluster.sh and their details
        args[3] = Program name		- 	Should match with the application name in the server. Ex: WordCount. Not Word Count
     */
    public static void main(String[] args) throws IOException {

        if (args.length != 4) {

            System.out.println("Not enough arguments passed");
            System.exit(1);
        } else {
            //TODO: displayCorrectUsage();
            //TODO: exit at this point
        }

        String inputDataLocation = args[0];
        String outputDataLocation = args[1];
        String instancesFile = args[2];
        String programName = args[3];

        String[] inputDataLocationSplits = inputDataLocation.split("//")[1].split("/");
        inputBucket = inputDataLocationSplits[0];
        String inputFolderInBucket = inputDataLocationSplits[1];

        String[] outputDataLocatonSplits = outputDataLocation.split("//")[1].split("/");
        String outputBucket = outputDataLocatonSplits[0];
        String outputFolderInBucket = outputDataLocatonSplits[1];


        //Read the instances.txt file
        System.out.println("Reading instances.txt file");
        Scanner sc = new Scanner(new File(instancesFile));
        int instances_num = Integer.parseInt(sc.nextLine());
        System.out.println("Instances count: " + instances_num);


        int count = 0;

        String ips[] = new String[instances_num];
        String instanceIp = "";
        TextSocket[] connections = new TextSocket[instances_num];

        //TODO: Convert this sequential code to parallel using Threads
        while (sc.hasNextLine()) {
            String[] line = sc.nextLine().split(";");
            String instance_id = line[0];
            instanceIp = line[1];
            ips[count] = instanceIp;

            System.out.println("Establishing connection to: " + instanceIp);
            TextSocket conn = new TextSocket(instanceIp, 3002);

            System.out.println("Connection established..");
            connections[count] = conn;

            count++;

            //send corresponding folder in s3 for this instance
            conn.putln(inputBucket);

            //send instance ip
            conn.putln(instanceIp);

            //Send the same output folder for all instances
            conn.putln(outputBucket);

            System.out.println("Program started on" + instanceIp);
        }

        divideFilesInS3(instances_num, ips,"input");
        startMappersPhase(connections);
        waitForMappersPhaseCompletion(connections);

        //TODO: Downloading files completed. TODO merge the common files
        downloadIntermediateFilesFromS3(ips);

        //Merging files to allTempFiles folder
        mergeIntermediateFilesFromS3();

        //Upload to output directory in the input_bucket
        uploadFilesToS3("allTempFiles/merged", outputBucket, "output");
        divideFilesInS3(instances_num, ips,"output");

        //TODO: divide merged files equally for all instances and push to S3 accordingly

        startReducersPhase(connections);
        closeConnections(connections);

        downloadOutputPartFilesFromS3(ips, outputBucket);

    }

}

