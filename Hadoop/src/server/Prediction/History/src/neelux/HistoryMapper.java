package neelux;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class HistoryMapper extends Mapper<Object, Text, Text, Text> {
	
	private static int hhmmDiff (String arr, String dep){
		if(arr.length()==3){
			arr='0'+arr;
		}
		if(dep.length()==3){
			dep='0'+dep;
		}
		if (Integer.parseInt(arr) > Integer.parseInt(dep)){
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2))) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		} else {
			return (Integer.parseInt(arr.substring(0, 2)) - Integer.parseInt(dep.substring(0, 2)) + 24) * 60 +
					(Integer.parseInt(arr.substring(2, 4)) - Integer.parseInt(dep.substring(2, 4)));
		}
	}
	
	
    public boolean validRow(String[] row){
    	try{
    		float arrDelay = Float.parseFloat(row[42]);
    		
    		int year = Integer.parseInt(row[0]);
    		
			int month = Integer.parseInt(row[2]);
			if(month<1 || month>12){
				return false;
			}
			
			int dayOfWeek = Integer.parseInt(row[4]);
			if(month<1 || month>7){
				return false;
			}
			
			String carrier = row[6];
			if (carrier.length()!=2){
				return false;
			}
			
			int originAirportId = Integer.parseInt(row[11]);
			if (originAirportId <= 0) return false;
			
			int destAirportId = Integer.parseInt(row[20]);
			if (destAirportId <= 0) return false;
			
			String crsDepTime = row[29];
			if (crsDepTime.equals("") || crsDepTime.equals("0")){
				return false;
			}
			
			int distanceGroup = Integer.parseInt(row[55]);
			if(distanceGroup<0){
				return false;
			}
						
		} catch(Exception e) {
			return false;
		}
    	return true;
    }
    
    public String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<String>();
        StringBuffer sb = new StringBuffer();
        boolean inQuote = false;
        char curChar;
        for (int i = 0; i < line.length(); i++) {
            curChar = line.charAt(i);
            if (inQuote) {
                if (curChar == '"') {
                    inQuote = false;
                } else {
                    sb.append(curChar);
                }
            } else {
                if (curChar == '"') {
                    inQuote = true;
                } else if (curChar == ',') {
                    values.add(sb.toString());
                    sb = new StringBuffer();
                } else {
                    sb.append(curChar);
                }
            }
        }
        values.add(sb.toString());  // last field
        return values.toArray(new String[1]);
    }
    
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    	String eachLine = value.toString();
    	String[] row = parseCSVLine(eachLine);
    	
    	if(row.length==110){
    		if(validRow(row)){
    			Text customKey = new Text();
    			Text customValue = new Text();
    			
    			customKey.set(row[3]);
    			customValue.set(row[42]+","+row[2]+","+row[4]+","+row[6]+","+row[11]+","+row[20]+","+row[29]+","+row[55]);
    			context.write(customKey,customValue);
    		}
    	}
    }
}
