package main;

import java.io.*;
import textsock.*;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;

public class WebServer {

    static private AWSCredentials credentials = new BasicAWSCredentials("***", "***");
    static private AmazonS3 s3client = new AmazonS3Client(credentials);
    static private String input_bucket;
    static private String input_file;
    static private String output_file;
    static private String output_bucket;
    static private ArrayList<String> sortList = new ArrayList<String>();
    static private ArrayList<String> allList = new ArrayList<String>();
    static private int port;
    static private String instance_num;

    public static String[] parseCSVLine(String line) {
        List<String> columns = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        boolean qStart = false;
        char cur;
        for (int i = 0; i < line.length(); i++) {
            cur = line.charAt(i);
            if (qStart) {
                if (cur == '"') {
                    qStart= false;
                } else {
                    sb.append(cur);
                }
            } 
            else {
                if (cur == ',') {
                    columns.add(sb.toString());
                    sb = new StringBuffer();
                } else if (cur == '"') {
                    qStart = true;
                } else {
                    sb.append(cur);
                }
            }
        }
        columns.add(sb.toString());

        return columns.toArray(new String[1]);
    }

    //TODO: add validation for values[8]
    private static boolean isSane(String fd) {
        try{
            int stringChk = Integer.parseInt(fd);
            return true;
        }
        catch(Exception ne){
            return false;
        }
    }
    private static String clean(String str) {
        return str.trim().replace("\"", "");
    }

    private static void sort()
    {
        try
        {
            Collections.sort(allList,new Comparator<String>(){
                public int compare(String p1, String p2) {
                    Integer t1=Integer.parseInt(p1.split("\t")[0]);
                    Integer t2=Integer.parseInt(p2.split("\t")[0]);
                    return t2.compareTo(t1);
                }
            });

            for(int i=0;i<10;i++)
            {
                sortList.add(allList.get(i));
            }
            allList.clear();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }

    private static void writeToOutputFile(String bucket,String prefix) throws IOException
    {
        Collections.sort(sortList,new Comparator<String>(){
            public int compare(String p1, String p2) {
                Integer t1=Integer.parseInt(p1.split("\t")[0]);
                Integer t2=Integer.parseInt(p2.split("\t")[0]);
                return t2.compareTo(t1);
            }
        });

        File outFile = new File("output.txt");
        FileWriter fstream = new FileWriter(outFile);
        BufferedWriter out = new BufferedWriter(fstream);

        for (int i=0;i<10;i++)
        {
         System.out.println(sortList.get(i)); //this statement prints out my keys and values
         out.write(sortList.get(i)+"\n");
         out.flush();   // Flush the buffer and write all changes to the disk
        }

        try{

            s3client.putObject(new PutObjectRequest(bucket, prefix+"/output-"+instance_num, outFile)); 
        }
        catch(Exception e)
        {
            System.out.println("Excepton");
            e.printStackTrace();
        }
    }

    private static void addFileFromS3(String input_file)
    {
        S3Object s3object = new S3Object();
        try
        {
            //System.out.println("Downloading an object");
            System.out.println("Input file : "+input_file);
            s3object = s3client.getObject(new GetObjectRequest(input_bucket, input_file));
            InputStream is = new GZIPInputStream(s3object.getObjectContent());
            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
            while(true)
            {
              String line = reader.readLine();
              if(line == null) break;
              String[] values = parseCSVLine(line);
              if(isSane(values[8]))
              {
                allList.add(clean(values[8])+"\t"+clean(values[0])+"\t"+clean(values[1])+"\t"+clean(values[2])+"\t"+clean(values[9])+"\t"+clean(values[10]));
              }
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

    /*  args[0] = port number
    */
    public static void main(String[] args) throws Exception 
    {
        port = 3002;

        TextSocket.Server svr = new TextSocket.Server(port);
        TextSocket conn;
        

        System.out.println("Socket created at port : "+port);
        while (null != (conn = svr.accept())) {
            System.out.println("Server is listening....");
            

            input_bucket = conn.getln();
            input_file = conn.getln();
            output_bucket = conn.getln();
            output_file = conn.getln();
            instance_num=conn.getln();
            String temp=conn.getln(); 

            while(!temp.equals(""))
            {
                addFileFromS3(temp);
                sort();
                temp=conn.getln();
            }

            //sort();

            System.out.println("Writing to an output file... ");
            writeToOutputFile(output_bucket,output_file);

            
            conn.putln("OK");
            System.out.println("Closing socket...");
            conn.close();
            svr.close();
            break;
        }    

            

    }
}