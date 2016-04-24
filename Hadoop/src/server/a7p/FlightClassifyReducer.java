package a7p;

import hadoop.*;

import java.io.IOException;
import java.util.HashMap;

//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Reducer;

import weka.classifiers.bayes.NaiveBayes;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class FlightClassifyReducer extends Reducer<Text,FlightWritable,Text,Text>{
	private Text result = new Text();
	private Text redKey = new Text();

	private Instances trainInstns;
	private NaiveBayes nb=new NaiveBayes();
	FastVector attributes = new FastVector();
	HashMap<String,Double> predictions=new HashMap<String,Double>();

	public void setup(Context context)
	{
		attributes.addElement(new Attribute("Month"));
		attributes.addElement(new Attribute("DepartureHour"));
		attributes.addElement(new Attribute("WeekDay"));
		attributes.addElement(new Attribute("Distance"));
		attributes.addElement(new Attribute("MonthDay"));
		FastVector possibleClasses = new FastVector(2);
		possibleClasses.addElement("1");
		possibleClasses.addElement("0");
		attributes.addElement(new Attribute("Delayed",possibleClasses));
		trainInstns=new Instances("Train",attributes,0);
		trainInstns.setClassIndex(trainInstns.numAttributes()-1);
		try {
			nb.buildClassifier(trainInstns);
		} catch (Exception e) {
			e.printStackTrace();
		}	
	}

	public void reduce(Text key, Iterable<FlightWritable> values,Context context) throws IOException, InterruptedException {

		for (FlightWritable fl : values) {
			Instance inst=new Instance(trainInstns.numAttributes());
			inst.setDataset(trainInstns);
			inst.setValue(0, fl.getMonth());
			inst.setValue(1, getHH(fl.getDepartHour()));
			inst.setValue(2, fl.getWeekday());
			inst.setValue(3, fl.getDistanceGroup());
			inst.setValue(4, fl.getMonthday());
			if(fl.getIsModel())
			{
				inst.setValue(5, fl.getDelayed()+"");
				build(inst);
			}
			else
			{
				predict(inst,fl);
			}
		}
	}


	public void build(Instance Inst) throws IOException, InterruptedException
	{
		try
		{
			nb.updateClassifier(Inst);
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}

	public void predict(Instance inst,FlightWritable fl)
	{
		try 
		{
			double res=nb.classifyInstance(inst);
			String key=(fl.getNumber()+"_"+fl.getDate()+"_"+getHHmm(fl.getDepartHour()));
			if(predictions.containsKey(key))
				predictions.put(key, predictions.get(key)+res);
			else
				predictions.put(key, res);
		}
		catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		System.out.println("Started cleanup");
		for(String key:predictions.keySet())
		{
			String res=predictions.get(key)<1.5?"TRUE":"FALSE";
			redKey.set(key);
			result.set(res);
			context.write(redKey,result);
		}
	}

	public int getHH(String deptime) {
		String hours="0000"+deptime;
		return Integer.parseInt(hours.substring(hours.length()-4,hours.length()-2));
	}

	public String getHHmm(String deptime) {
		String hours="0000"+deptime;
		return hours.substring(hours.length()-4);
	}
}

