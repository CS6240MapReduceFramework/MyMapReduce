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

public class MapReduce2{


	public static class FlightStatsMapper extends Mapper<Object, Text, Text, Text>{
		private Text carrier_a = new Text();
		private Text carrier_d = new Text();
		private Text stats_a =new Text();
		private Text stats_d = new Text();

		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {

			String[] line =parseCSVLine(value.toString());
			if(stringCheck(line)){
				//airline - month - day - origin 
				carrier_a.set(clean(line[6])+"\t"+clean(line[2])+"\t"+clean(line[3])+"\t"+clean(line[23]));
				//other mapper : airline - month - day - destination 
				carrier_d.set(clean(line[6])+"\t"+clean(line[2])+"\t"+clean(line[3])+"\t"+clean(line[14]));

				//14-orig;23-dest;29-crsDep;30-dep;40-crsArr;41-Arr; 5-flightDate; 10 - flightnum
				stats_a = new Text("A"+"\t"+clean(line[14])+"\t"+clean(line[40].trim())+"\t"+clean(line[5])+"\t"+clean(line[10]));
				stats_d = new Text("D"+"\t"+clean(line[23])+"\t"+clean(line[29].trim())+"\t"+clean(line[5])+"\t"+clean(line[10]));
		

				context.write(carrier_a, stats_a);
				context.write(carrier_d, stats_d);
			}
		}

		private static String clean(String str) {
			return str.trim().replace("\"", "");
		}

		public static Boolean stringCheck(String[] fd)
		{
			return ((fd[6]!="") && (fd[2]!="") && (fd[3]!="") && (fd[23]!="") &&
					(fd[14]!="") &&  (fd[40].trim()!="") && (fd[10]!="") && (fd[10]!="NA") &&(fd[29].trim()!="") && (fd[5].trim()!="")) && ((fd[6]!="NA") && (fd[2]!="NA") && (fd[3]!="NA") && (fd[23]!="NA") &&
					(fd[14]!="NA") &&  (fd[40].trim()!="NA") &&(fd[29].trim()!="NA") && (fd[5].trim()!="NA"));
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
	}

	public static class FlightStatsReducer
	extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();

		class FlightData
		{
			String city;
			int mins;
 			Date date;
 			String flightnum;
		}
		public void reduce(Text key, Iterable<Text> values,	Context context) throws IOException, InterruptedException {

			String[] keyStr = key.toString().split("\t");

			List<FlightData> arrlist = new ArrayList<FlightData>();
			List<FlightData> depList = new ArrayList<FlightData>();
			for(Text flight: values)
			{
				String[] data = flight.toString().split("\t");
				if(data.length>1){
					try{
						FlightData flightData = new FlightData();
						flightData.city = data[1];
						flightData.mins = getMins(data[2]);
 						flightData.date = toDate(data[3],data[2]);
 						flightData.flightnum = data[4];
						if(data[0].equals("A"))
						{
							arrlist.add(flightData);
						}
						else
						{
							depList.add(flightData);
						}
					}catch(Exception e){
					}
				}
			}

			for(FlightData arrivals : arrlist)
			{
				for(FlightData departures : depList)
				{
					long layover = dateDiff(arrivals.date,departures.date);
					if(layover>=30 && layover< 60 && !arrivals.city.equalsIgnoreCase(departures.city))
					{
						result.set(departures.city+"\t"+layover+"\t"+departures.flightnum);
						Text a = new Text();
						a.set(key.toString()+"\t"+arrivals.city+"\t"+arrivals.flightnum);
						context.write(a,result);
					}
				}
			}
			}
		}

	public static long dateDiff(Date arrivalTime, Date departureTime)
	{
		return (departureTime.getTime() - arrivalTime.getTime())/(1000*60);
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

	public static int getMins(String time)
	{
		time = "0000"+time;
		int length = time.length();
 		int hh = Integer.parseInt(time.substring(length-2));
		int mm = Integer.parseInt(time.substring(length-4,length-2));
		int mins= hh*60 + mm;
 		return mins;
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		//conf.set("my.dijkstra.parameter", "value");
		Job job = Job.getInstance(conf, "FLight Data");
		job.setJarByClass(MapReduce2.class);
		job.setMapperClass(FlightStatsMapper.class);
		//job.setCombinerClass(FlightStatsReducer.class);
		job.setReducerClass(FlightStatsReducer.class);
		// job.setNumReduceTasks(4);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}
}
