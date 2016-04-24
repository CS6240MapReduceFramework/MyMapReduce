package a7p;

import hadoop.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

//import org.apache.hadoop.io.BooleanWritable;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.io.Writable;

public class FlightWritable implements Writable{
	
	public boolean getIsSane() {
		return isSane;
	}
	public void setIsSane(boolean isSane) {
		this.isSane = isSane;
	}
	public String getYear() {
		return year.toString();
	}
	public void setYear(String year) {
		this.year = new Text(year);
	}
	public int getMonth() {
		return month.get();
	}
	public void setMonth(int month) {
		this.month = new IntWritable(month);
	}
	public int getWeekday() {
		return weekday.get();
	}
	public void setWeekday(int weekday) {
		this.weekday = new IntWritable(weekday);
	}
	public String getDate() {
		return date.toString();
	}
	public void setDate(String dtString) {
		String date=formatDate(dtString);
		this.date = new Text(date);
	}
	public String getCarrier() {
		return carrier.toString();
	}
	public void setCarrier(String carrier) {
		this.carrier = new Text(carrier);
	}
	public String getOrigin() {
		return origin.toString();
	}
	public void setOrigin(String origin) {
		this.origin = new Text(origin);
	}
	public String getDestination() {
		return destination.toString();
	}
	public void setDestination(String destination) {
		this.destination = new Text(destination);
	}
	public String getNumber() {
		return number.toString();
	}
	public void setNumber(String number) {
		this.number = new Text(number);
	}
	public String getDepartHour() {
		return departHour.toString();
	}
	public void setDepartHour(String departHour) {
		this.departHour = new Text(departHour);
	}
	public int getDistanceGroup() {
		return distanceGroup.get();
	}
	public void setDistanceGroup(int distanceGroup) {
		this.distanceGroup = new IntWritable(distanceGroup);
	}
	public boolean getCancelled() {
		return cancelled;
	}
	public void setCancelled(boolean cancelled) {
		this.cancelled = cancelled;
	}
	public boolean getIsModel() {
		return isModel;
	}
	public void setIsModel(boolean isModel) {
		this.isModel = isModel;
	}
	public int getDelayed() {
		return delayed.get();
	}
	public void setDelayed(int delayed) {
		this.delayed = new IntWritable(delayed);
	}
	
	public int getMonthday() {
		return monthday.get();
	}
	public void setMonthday(String dtString) {
		
		String[] date = formatDate(dtString).split("-");
		this.monthday=new IntWritable(Integer.parseInt(date[2]));
	}
	
	public String formatDate(String dtString)
	{
		SimpleDateFormat sdf=new SimpleDateFormat("MM/dd/yy");
		SimpleDateFormat df=new SimpleDateFormat("yyyy-MM-dd");
		try
		{
			return df.format(sdf.parse(dtString));
		}
		catch(ParseException pe)
		{
			return dtString;
		}
	}

	private boolean isSane;
	private Text year;
	private IntWritable month;
	private IntWritable weekday;
	private Text date;
	private Text carrier;
	private Text origin;
	private Text destination;
	private Text number;
	private Text departHour;
	private IntWritable distanceGroup;
	private boolean cancelled;
	private IntWritable monthday;

	private BooleanWritable isModel;

	private IntWritable delayed;

	public FlightWritable()
	{
		this.isSane=false; 
		this.year=new Text();
		this.month=new IntWritable();
		this.weekday=new IntWritable();
		this.monthday=new IntWritable();
		this.date=new Text();
		this.carrier=new Text();
		this.number=new Text();
		this.origin=new Text();
		this.destination=new Text();
		this.departHour=new Text();
		this.distanceGroup=new IntWritable();
		this.cancelled= false;
		this.delayed=new IntWritable(0);
		this.isModel=false;
		
	}
	public FlightWritable(String line)
	{
		set(line);
	}
	public FlightWritable(FlightWritable fl)
	{
		this.isSane=fl.getIsSane();
		this.year=new Text(fl.getYear());
		this.month=new IntWritable(fl.getMonth());
		this.weekday=new IntWritable(fl.getWeekday());
		this.monthday=new IntWritable(fl.getMonthday());
		this.date=new Text(fl.getDate());
		this.carrier=new Text(fl.getCarrier());
		this.number=new Text(fl.getNumber());
		this.origin=new Text(fl.getNumber());
		this.destination=new Text(fl.getDestination());
		this.departHour=new Text(fl.getDepartHour());
		this.distanceGroup=new IntWritable(fl.getDistanceGroup());
		this.cancelled=new BooleanWritable(fl.getCancelled());
		this.delayed=new IntWritable(fl.getDelayed());
		this.isModel=new BooleanWritable(fl.getIsModel());
	}

	public void set(String line){

		String fd[]=parseCSVLine(line);
		if(isSane(fd))
		{
			setIsSane(true);
			setYear(clean(fd[1]));
			setMonth(toNum(fd[2]));
			setWeekday(toNum(fd[3]));
			setMonthday(clean(fd[4]));
			setDate(clean(fd[4]));
			setCarrier(clean(fd[5]));
			setNumber(clean(fd[6]));
			setOrigin(clean(fd[7]));
			setDestination(clean(fd[8]));
			setDepartHour(clean(fd[9]));
			setDistanceGroup(toNum(fd[10]));
			if(clean(fd[13]).equals("M"))
			{
				setCancelled(toNum(fd[11])==1);
				setDelayed(toDub(fd[12])>0.0?1:0);
				setIsModel(true);
			}
			else
			{
				setCancelled(false);
				setDelayed(0);
				setIsModel(false);
			}
		}

	}

	@Override
	public void readFields(DataInput in) throws IOException {
		isSane.readFields(in);
		year.readFields(in);
		month.readFields(in);
		weekday.readFields(in);
		monthday.readFields(in);
		date.readFields(in);
		carrier.readFields(in);
		number.readFields(in);
		origin.readFields(in);
		destination.readFields(in);
		departHour.readFields(in);
		distanceGroup.readFields(in);
		cancelled.readFields(in);
		delayed.readFields(in);
		isModel.readFields(in);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		isSane.write(out);
		year.write(out);
		month.write(out);
		weekday.write(out);
		monthday.write(out);
		date.write(out);
		carrier.write(out);
		number.write(out);
		origin.write(out);
		destination.write(out);
		departHour.write(out);
		distanceGroup.write(out);
		cancelled.write(out);
		delayed.write(out);
		isModel.write(out);
	}

	public Boolean saneCheck(String[] fd)
	{
		if(clean(fd[13]).equals("M"))
			toNum(fd[12]);

		return (toNum(fd[8])>0 && toNum(fd[9])>0 && toNum(fd[7])>0 && toNum(fd[6])>0 &&
				toNum(fd[10])>=0 && toNum(fd[2])>0 && toNum(fd[3])>0);
	}

	private boolean isSane(String[] fd) {
		try{
			return fd.length==14 && saneCheck(fd);
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


	private double toDub(String str) {
		String cleaned=clean(str);
		if(cleaned.equals(""))
			return 0.0;
		else
			return Double.parseDouble(cleaned);
	}

	public Integer toNum(String str)
	{
		String cleaned=clean(str);
		if(cleaned.equals(""))
			return 0;
		else
			return Integer.parseInt(clean(str));
	}

	public String clean(String str) {
		return str.trim().replace("\"", "");
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
