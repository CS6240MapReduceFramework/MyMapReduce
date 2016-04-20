package hadoop;

import java.io.BufferedReader;
import com.amazonaws.*;
import org.apache.commons.logging.*;
import com.amazonaws.auth.*;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.auth.*;
import com.amazonaws.services.s3.*;
import com.amazonaws.services.s3.model.*;

import textsock.TextSocket;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;


import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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

	public void cleanDirectory(File directory)
	{
		for(File f : directory.listFiles())
			f.delete();

	}
	public void cleanUpFiles()
	{
		cleanDirectory(new File(job.conf.prop.getProperty("TEMP_DIR")));
		cleanDirectory(new File(job.conf.prop.getProperty("OUTPUT_DIR")));

	}
	
	
	public void reducerTask() throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException
	{


		Class[] cArgs = new Class[3];
		cArgs[0] = String.class;
		cArgs[1] = ArrayList.class;
		cArgs[2] = Context.class;
		Method reduceMethod = job.reducerCls.getMethod("reduce",cArgs);
		Context reduceContext = new Context();
		String part_output_file = job.conf.prop.getProperty("OUTPUT_DIR")+"part-r-00000";
		reduceContext.setup(part_output_file);
		HashMap<String, ArrayList<Integer>> wordMap = new HashMap<>();
		
		String tempDir = job.conf.prop.getProperty("TEMP_DIR");
		File tempFiles = new File(tempDir);
		job.partFiles = tempFiles.listFiles();

		for(int i=0; i<job.partFiles.length;i++)
		{
			FileReader fileReader = new FileReader(job.partFiles[i]);
			BufferedReader bufferedReader = new BufferedReader(fileReader);

			String line = null;
			while((line = bufferedReader.readLine())!= null)
			{
				String[] lines = line.split(" ");
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

//	private static void getFileFromS3(String input_file)
//    {
//        s3object s3object = new S3Object();
//        try
//        {
//            
//            System.out.println("Input file : "+input_file);
//            s3object = s3client.getObject(new GetObjectRequest(input_bucket, input_file));
//            InputStream is = new GZIPInputStream(s3object.getObjectContent());
//            BufferedReader reader = new BufferedReader(new InputStreamReader(is));
//            while(true)
//            {
//              String line = reader.readLine();
//              if(line == null) break;
//              String[] values = parseCSVLine(line);
//              if(isSane(values[8]))
//              {
//                allList.add(clean(values[8])+"\t"+clean(values[0])+"\t"+clean(values[1])+"\t"+clean(values[2])+"\t"+clean(values[9])+"\t"+clean(values[10]));
//              }
//            }
//        }
//        catch(AmazonServiceException ase)
//        {
//          System.out.println( "AmazonServiceException" );
//          ase.printStackTrace();
//        }
//        catch(AmazonClientException ace)
//        {
//          System.out.println( "AmazonClientException" );
//          ace.printStackTrace();
//        }
//        catch(Exception e)
//        {
//            System.out.println("Excepton");
//            e.printStackTrace();
//        }
//    }
	public void mapperTask()
	{
		System.out.println("mapper task called");
	}
	
	public void mapperTask(File inputDir) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, Exception
	{
		
		
		
		
		String line = null;
		Class[] cArgs = new Class[3];
		cArgs[0] = Object.class;
		cArgs[1] = String.class;
		cArgs[2] = Context.class;
				
		Method mapMethod = job.mapperCls.getMethod("map",cArgs);
		
		Context mapContext = new Context();
		String temp_map_file = job.conf.prop.getProperty("TEMP_DIR")+"part-temp-file";
		mapContext.setup(temp_map_file);
	
		for(File inputFile : inputDir.listFiles())
		{
			FileReader fileReader = new FileReader(inputFile);
			BufferedReader bufferedReader = new BufferedReader(fileReader);
			
			while((line = bufferedReader.readLine())!= null)
			{
				mapMethod.invoke(job.mapperInstance,new Object(),line,mapContext);
				
			}
			bufferedReader.close();
		}
		
		
	}

	public void waitForCompletion(Boolean bool) throws NoSuchMethodException, SecurityException, IOException, IllegalAccessException, IllegalArgumentException, InvocationTargetException, Exception
	{

		//Cleans TEMP_DIR and OUTPUT_DIR
		//cleanUpFiles();

		

		int port = 3002;

        TextSocket.Server svr = new TextSocket.Server(port);
        TextSocket conn;

        while (null != (conn = svr.accept())) {
        System.out.println("Server is listening....");
            

            String programName = conn.getln();
            String input_bucket = conn.getln();

           // getFileFromS3(input_bucket);
            String output_bucket = conn.getln();
        

            if(conn.getln().equals("MAPPER_START"))
            {
                
            	mapperTask();
            }

            conn.putln("MAPPER_COMPLETE");

            if(conn.getln().equals("REDUCER_START"))
            {
            	reducerTask();
            }            


            conn.putln("REDUCER_COMPLETE");

            
            System.out.println("Closing socket...");
            conn.close();
            svr.close();
            break;
        }    

	}
}
