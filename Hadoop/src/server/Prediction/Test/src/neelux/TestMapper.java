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


public class TestMapper extends Mapper<Object, Text, Text, Text> {
	
    public boolean validRow(String[] row){
    	try{
    		int year = Integer.parseInt(row[1]);
    		
			int month = Integer.parseInt(row[3]);
			if(month<1 || month>12){
				return false;
			}
			
			int dayOfWeek = Integer.parseInt(row[5]);
			if(month<1 || month>7){
				return false;
			}
			
			String carrier = row[7];
			if (carrier.length()!=2){
				return false;
			}
			
			int originAirportId = Integer.parseInt(row[12]);
			if (originAirportId <= 0) return false;
			
			int destAirportId = Integer.parseInt(row[21]);
			if (destAirportId <= 0) return false;
			
			String crsDepTime = row[30];
			if (crsDepTime.equals("") || crsDepTime.equals("0")){
				return false;
			}
			
			int distanceGroup = Integer.parseInt(row[56]);
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
    	
//    	if(row.length==112){
//    		if(validRow(row)){
    			Text customKey = new Text();
    			Text customValue = new Text();
    			String fl_date = row[6];
				String[] dateParts = fl_date.split("/");
				if(dateParts.length == 3){
					fl_date = dateParts[0]+"-"+dateParts[1]+"-"+dateParts[2];
				}
    			customKey.set(row[4]);
    			customValue.set(row[11]+","+fl_date+","+row[3]+","+row[5]+","+row[7]+","+row[12]+","+row[21]+","+row[30]+","+row[56]);
    			context.write(customKey,customValue);
//    		}
//    	}
    }
}
