import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Main {

	public static class FlightStatsMapper extends Mapper<Object, Text, Text, Text>{
		private Text carrier = new Text();
		private Text duration =new Text();

		
		private static String clean(String str) {
			return str.trim().replace("\"", "");
		}

		private boolean isSane(String[] fd) {
			try{
				return fd.length==110 && stringCheck(fd) && intCheck(fd) && zoneCheck(fd) && timeCheck(fd);
			}
			catch(NumberFormatException ne){
				return false;
			}
			catch(ArrayIndexOutOfBoundsException le){
				return false;	
			}
			catch(Exception e){
				return false;
			}
		}

		public static Boolean stringCheck(String[] fd)
		{
			return ((fd[14]!="") && (fd[15]!="") && (fd[16]!="") && (fd[18]!="") &&
					(fd[23]!="") &&  (fd[24]!="") &&(fd[25]!="") && (fd[27]!="") && 
					(fd[29]!="") && (fd[30]!="") && (fd[40]!="") && (fd[41]!=""));
		}

		public static Boolean intCheck(String[] fd)
		{
			return (toNum(fd[11])>0 && toNum(fd[12])>0 && toNum(fd[13])>0 && toNum(fd[19])>0 && toNum(fd[17])>0 &&
					toNum(fd[20])>0 && toNum(fd[21])>0 && toNum(fd[22])>0 && toNum(fd[28])>0 && toNum(fd[26])>0 && 
					toInt(fd[29])>0) && toInt(fd[30])>0 && toInt(fd[40])>0 && toInt(fd[41])>0;
		}
		public static Double toNum(String str)
		{
			return Double.parseDouble(clean(str));
		}
		public static Integer toInt(String str)
		{
			return Integer.parseInt(clean(str));
		}

		public static Boolean zoneCheck(String[] fd)
		{
			return (toInt(fd[40])!=0 && toInt(fd[29])!=0 && toInt(fd[30])!=0 && toInt(fd[41])!=0 && 
					(toMin(fd[40])-toMin(fd[29])-toInt(fd[50]))%60==0.0);
		}
		public static int toMin(String hhmm)
		{
			int m=Integer.valueOf(hhmm.trim());
			return (m/100)*60+m%100;
		}

		public static Boolean timeCheck(String[] fd)
		{
			int crs_arr=fd[40].equals("")?0:toMin(fd[40]);
			int crs_dep=fd[29].equals("")?0:toMin(fd[29]);
			double crs_el=fd[50].equals("")?0:toNum(fd[50]);
			int arr_time=fd[41].equals("")?0:toMin(fd[41]);
			int dep_time=fd[30].equals("")?0:toMin(fd[30]);
			double act_el=fd[51].equals("")?0:toNum(fd[51]);
			crs_arr+=(crs_arr<=crs_dep)?1440:0;
			arr_time+=(arr_time<=dep_time)?1440:0;
			return (toNum(fd[47])!=0) || ((arr_time-dep_time-act_el)==(crs_arr-crs_dep-crs_el)) && delayCheck(fd);
		}

		public static Boolean delayCheck(String[] fd)
		{
			float arrDel=fd[42].equals("")?0:Float.parseFloat(fd[42]);
			float arrDelMin=fd[43].equals("")?0:Float.parseFloat(fd[43]);
			float arrDel15=fd[44].equals("")?0:Float.parseFloat(fd[44]);
			return ((arrDel>0.0 && arrDel==arrDelMin) || (arrDel<=0.0 && arrDelMin==0.0)) && 
					((arrDelMin<15.0) || (arrDelMin>=15.0 && arrDel15==1.0));
		}


		public String[] parseCSVLine(String line) {
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

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] line =parseCSVLine(value.toString());
			if(isSane(line)) {
				//Unique_carrier - Month - Destination - Origin
				carrier.set(clean(line[6])+"\t"+clean(line[2])+"\t"+clean(line[23])+"\t"+clean(line[14]));

				//ACTUAL_ELAPSED_TIME
				duration = new Text(clean(line[50]));
				context.write(carrier, duration);
			}
		}
		
	}

	public static class FlightStatsReducer
	extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {
			
			HashMap<String,ArrayList<Text>> fMap = new HashMap<String,ArrayList<Text>>();
			ArrayList<Text> gFlights = new ArrayList<Text>();

			int sum = 0;
			int numberOfFlights = 1;

			for(Text duration : values)
			{
				sum+= Integer.parseInt(duration.toString());
				numberOfFlights++;
			}

			double avgDuration = sum/numberOfFlights;
			
			String res = avgDuration+"";
			result.set(res);
			context.write(key, result);

		}
	}

	public static Date toDate(String date, String mins)
	{
		mins="0000"+mins;
		int len=mins.length();
		String minsFormat =mins.substring(len-4,len-2)+":"+mins.substring(len-2,len)+":00";
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		date=date+" "+minsFormat;
		Date d1 = null;
		try {
			d1 = format.parse(date);

		} catch (ParseException e) {
			e.printStackTrace();
		}    
		return d1;
	}


	public static long dateDiff(String depDate, String depMin, String arrDate, String arrMin, String dep)
	{
		Date arrDated = toDate(arrDate,arrMin);
		Date depDated = toDate(depDate,depMin);
		
		//System.out.println("date before: "+arrDated.toString());
		if(Integer.parseInt(arrMin) < Integer.parseInt(dep))
		{
			Calendar cal = Calendar.getInstance();
			cal.setTime(arrDated);
			cal.add(Calendar.DATE, 1);
			arrDated = cal.getTime();
			
			System.out.println("date incremented: "+arrDated.toString());
		}
		
		// Get msec from each, and subtract.
		long diff = depDated.getTime() - arrDated.getTime();
		long diffMinutes = diff / (60 * 1000);  
		return diffMinutes;

	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FLight Data");
		job.setJarByClass(Main.class);
		job.setMapperClass(FlightStatsMapper.class);
		job.setReducerClass(FlightStatsReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);

		// Counters counters = job.getCounters();

		// long totConns =  counters.findCounter(COUNTERS.CONNS).getValue();
		// long totMisses = counters.findCounter(COUNTERS.MISSES).getValue();

		// System.out.println("Connections:"+ totConns+" Missed connections: "+ totMisses);
		// try
		// {
		// 	System.out.println("Missed Percent:"+ (totMisses/totConns)*100);
		// }
		// catch(ArithmeticException e)
		// {
		// 	System.out.println("EXCEPTION : No missed connections were found. ");
		// }
		
		
	}
}
