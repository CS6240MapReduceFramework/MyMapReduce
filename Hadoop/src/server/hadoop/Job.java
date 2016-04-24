package hadoop;

import java.io.*;

import com.amazonaws.*;
import org.apache.commons.logging.*;
import org.apache.commons.io.*;
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
import java.util.*;

public class Job {

    private String jobname;
    public Object mapperInstance;
    public Class<?> mapperCls;
    public Class<?> partitionerCls;
    public Object partitionerInstance;
    public Class<?> reducerCls;
    public Class<?> outputKeyClass;
    public Class<?> outputValueClass;
    public Object reducerInstance;
    public String outputBucket;
    private Class jar;
    private static Job job;
    public int NUM_REDUCE_TASKS = 1;
    public int MAPPER_TASKS = 0;
    public int REDUCER_TASKS = 0;
    public double mapperPercentageComplete = 0.00d;
    public Configuration conf;
    public File[] partFiles;
    public static boolean reducerComplete = false;
    private static int MapRecordCount = 0;
    public static String instanceIp;
    static Properties prop = new Properties();

    static {
        try {
            //Properties file should be within src folder
            prop.load(new FileInputStream("config.properties"));
        } catch (Exception e) {
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

    public static Job getInstance(Configuration conf, String jobname) {
        job = new Job();
        job.jobname = jobname;
        job.conf = conf;

        return job;
    }

    public static int getMapRecordCount() {
        return MapRecordCount;
    }

    public void setMapperClass(Class<? extends Mapper> mapperClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        mapperCls = Class.forName(mapperClass.getName());
        mapperInstance = mapperCls.newInstance();
    }

    public void setPartitionerClass(Class<? extends Partitioner> partitionerClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        partitionerCls = Class.forName(partitionerClass.getName());
        partitionerInstance = partitionerCls.newInstance();
    }


    public void setNumReduceTasks(int reduceTasks) {
        job.NUM_REDUCE_TASKS = reduceTasks;
    }

    public void setReducerClass(Class<? extends Reducer> reducerClass) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        reducerCls = Class.forName(reducerClass.getName());
        reducerInstance = reducerCls.newInstance();

    }

    public void setOutputKeyClass(Class outKey) {
        outputKeyClass = outKey;
    }

    public void setOutputValueClass(Class outValue) {
        outputValueClass = outValue;
    }

    //TODO: Identify the purpose of this method
    /*
     * Identify the main class in the application and set the jar to the same.
     */
    public void setJarByClass(Class jarClass) throws ClassNotFoundException {
        ClassLoader classLoader = jarClass.getClassLoader();
        job.jar = classLoader.loadClass(jarClass.getName());
    }


    public boolean reducerTask(String outputBucket, String instanceIp) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException, Exception {

        getFileFromS3(outputBucket, instanceIp + "/merged", "reducelocal");

        Class[] cArgs = new Class[3];
        cArgs[0] = outputKeyClass;
        cArgs[1] = CustomIterable.class;
        cArgs[2] = Context.class;
        Method reduceMethod = job.reducerCls.getMethod("reduce", cArgs);
        Context reduceContext = new Context();
        reduceContext.foldername = "output";
        reduceContext.instance = instanceIp;
        reduceContext.phase = "REDUCER";

        File tempFiles = new File("reducelocal/" + instanceIp + "/merged");

        if (!tempFiles.exists()) {
            return false;
        }
        job.partFiles = tempFiles.listFiles();

        System.out.println("Reducer Started");

        File fdir = new File("output");
        if (!fdir.exists())
            fdir.mkdirs();

        File f = new File("output/part-" + instanceIp);
        if (!f.exists())
            f.createNewFile();

        for (int i = 0; i < job.partFiles.length; i++) {
            if (job.outputKeyClass.equals(IntWritable.class)) {

                IntWritable key = new IntWritable();
                key.set(Integer.parseInt(job.partFiles[i].getName()));

                Iterator itr = FileUtils.lineIterator(job.partFiles[i], "UTF-8");
                if (job.outputValueClass.equals(Text.class)) {
                    CustomIterable<Text> cIterable = new CustomIterable(itr);
                    cIterable.setDataType(job.outputValueClass);
                    reduceMethod.invoke(job.reducerInstance, key, cIterable, reduceContext);
                } else if (job.outputValueClass.equals(IntWritable.class)) {
                    CustomIterable<IntWritable> cIterable = new CustomIterable(itr);
                    cIterable.setDataType(job.outputValueClass);
                    reduceMethod.invoke(job.reducerInstance, key, cIterable, reduceContext);
                }

            } else if (job.outputKeyClass.equals(Text.class)) {
                Text key = new Text();
                key.set(job.partFiles[i].getName());

                Iterator itr = FileUtils.lineIterator(job.partFiles[i], "UTF-8");
                if (job.outputValueClass.equals(Text.class)) {
                    CustomIterable<Text> cIterable = new CustomIterable(itr);
                    cIterable.setDataType(job.outputValueClass);
                    reduceMethod.invoke(job.reducerInstance, key, cIterable, reduceContext);
                } else if (job.outputValueClass.equals(IntWritable.class)) {
                    CustomIterable<IntWritable> cIterable = new CustomIterable(itr);
                    cIterable.setDataType(job.outputValueClass);
                    reduceMethod.invoke(job.reducerInstance, key, cIterable, reduceContext);
                }
            }

        }
        return true;
    }


    //Upload a directory from local folder to given S3Folder
    public static void uploadToS3(String bucketName, String toS3Folder, String fromLocalFolder) {
        try {
            File from = new File(fromLocalFolder);
            if (reducerComplete) {
                MapRecordCount++;
            }
            System.out.println("Uploading files to S3 from a EC2 instance\n");

            TransferManager tx = new TransferManager(credentials);
            MultipleFileUpload mu = tx.uploadDirectory(bucketName, toS3Folder, from, true);

            System.out.println("Waiting while uploading to S3");
            mu.waitForCompletion();
            System.out.println("Files uploading to S3 completed");
        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            ace.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void getFileFromS3(String bucketName, String fromS3Folder, String toLocalFolder) {
        try {

            File localFolder = new File(toLocalFolder);
            if (!localFolder.exists())
                localFolder.mkdirs();

            TransferManager tx = new TransferManager(credentials);
            MultipleFileDownload md = tx.downloadDirectory(bucketName, fromS3Folder, localFolder);
            System.out.println("Waiting for files to download from S3");
            md.waitForCompletion();
            System.out.println("Files downloading from S3 to local folder completed");
        } catch (AmazonServiceException ase) {
            ase.printStackTrace();
        } catch (AmazonClientException ace) {
            ace.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean mapperTask(String inputBucket, String instanceIp) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, InterruptedException, Exception {

        getFileFromS3(inputBucket, instanceIp, "inputlocal");


        String line = null;

        Class[] cArgs = new Class[3];
        cArgs[0] = Object.class;
        cArgs[1] = Text.class;
        cArgs[2] = Context.class;

        System.out.println("Mapper class instantiated..");
        Method mapMethod = job.mapperCls.getMethod("map", cArgs);

//        Context<Text,IntWritable> mapContext = new MapperContext();
        Context mapContext = new Context();
        mapContext.instance = instanceIp;
        mapContext.foldername = instanceIp + "/tempFiles";
        mapContext.phase = "MAPPER";

        File inputDir = new File("inputlocal/" + instanceIp + "/input");

        if (!inputDir.exists())
            return false;
        System.out.println("input file path: " + inputDir.getAbsolutePath());


        System.out.println("for each file in inputlocal/" + instanceIp + "/input:");
        for (File inputFile : inputDir.listFiles()) {
            FileReader fileReader = new FileReader(inputFile);
            BufferedReader bufferedReader = new BufferedReader(fileReader);

            Text text = new Text();
            System.out.println("calling map method in mapper task");
            while ((line = bufferedReader.readLine()) != null) {
                text.set(line);
                mapMethod.invoke(job.mapperInstance, new Object(), text, mapContext);

            }
            bufferedReader.close();
        }
        return true;

    }

    public void waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, Exception {

        System.out.println("Mapreduce job started...");

        //Read the instances.txt file for port number
        Scanner sc = new Scanner(new File("instances.txt"));
        int instances_num = Integer.parseInt(sc.nextLine());
        int port = 3002;
        while (sc.hasNextLine()) {
            String[] line = sc.nextLine().split(";");
            System.out.println("IPAddress of this ec2 instance: " + line[1]);
            port = Integer.parseInt(line[2]);
        }

        String ip = InetAddress.getLocalHost().getHostAddress();

        System.out.println("Port : " + port);
        TextSocket.Server svr = new TextSocket.Server(port);

        TextSocket conn;

        while (null != (conn = svr.accept())) {
            System.out.println("Server is listening....");

            String inputBucket = conn.getln();

            //the instance ip address sent from the client is the input folder for this instance
            instanceIp = conn.getln();
            String portNum = conn.getln();
            instanceIp = instanceIp + "_" + portNum;
            outputBucket = conn.getln();

            String command = conn.getln();

            if (command.equals("MAPPER_START")) {
                boolean mapstatus = mapperTask(inputBucket, instanceIp);
                if (!mapstatus) {
                    System.out.println("No Input files for Mapper");
                    conn.putln("EXIT");
                } else {
                    System.out.println("Copying key files from Mapper to S3");
                    //copy temp files after Mapper to S3
                    uploadToS3(inputBucket, instanceIp + "/tempFiles", instanceIp + "/tempFiles");

                    System.out.println("key files from Mapper uploaded to S3");

                    conn.putln("MAPPER_COMPLETE");
                }
            } else
                System.out.println("Expected Command: MAPPER_START. Received command: " + command);

            if (conn.getln().equals("REDUCER_START")) {
                boolean reducestatus = reducerTask(inputBucket, instanceIp);
                if (!reducestatus) {
                    conn.putln("EXIT");
                } else {
                    this.reducerComplete = true;
                    uploadToS3(outputBucket, "output", "output");
                    conn.putln("REDUCER_COMPLETE");
                }
            }

            System.out.println("Closing socket...");
            conn.close();
            svr.close();
            break;
        }

    }
}
