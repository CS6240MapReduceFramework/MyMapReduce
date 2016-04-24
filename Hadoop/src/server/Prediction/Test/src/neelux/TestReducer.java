package neelux;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.FileOutputStream;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import weka.classifiers.trees.RandomForest;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

public class TestReducer extends Reducer<Text,Text,Text,Text> {
	public int getHour(String deptime) {
		String hours="0000"+deptime;
		return Integer.parseInt(hours.substring(hours.length()-4,hours.length()-2));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
		Text result = new Text();
		FastVector attributes = new FastVector();
		attributes.addElement(new Attribute("Month"));
		attributes.addElement(new Attribute("DayOfWeek"));
		attributes.addElement(new Attribute("Carrier"));
		attributes.addElement(new Attribute("OriginId"));
		attributes.addElement(new Attribute("DestId"));
		attributes.addElement(new Attribute("DepartureHour"));
		attributes.addElement(new Attribute("Distance"));
		FastVector possibleClasses = new FastVector(2);
		possibleClasses.addElement("0");
		possibleClasses.addElement("1");
		attributes.addElement(new Attribute("Delayed",possibleClasses));

		Instances testInstances = new Instances("test",attributes,0);
		testInstances.setClassIndex(testInstances.numAttributes()-1);
		RandomForest rf = new RandomForest();
		try {
			rf = (RandomForest) weka.core.SerializationHelper.read("../History/forests/"+key.toString()+".model");
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		for(Text val : values)
		{
			Instance inst=new Instance(testInstances.numAttributes());
			String[] valArray = val.toString().split(",");
			try{
				inst.setDataset(testInstances);
				inst.setValue(0, Integer.parseInt(valArray[2]));
				inst.setValue(1, Integer.parseInt(valArray[3]));
				inst.setValue(2, valArray[4].hashCode());
				inst.setValue(3, Integer.parseInt(valArray[5]));
				inst.setValue(4, Integer.parseInt(valArray[6]));
				inst.setValue(5, getHour(valArray[7]));
				inst.setValue(6, Integer.parseInt(valArray[8]));
				
				result.set(rf.classifyInstance(inst)+"");
			} catch (Exception e) {
				result.set("Error");
				//e.printStackTrace();
			}
			key.set(valArray[0]+"_"+valArray[1]+"_"+valArray[7]);
			context.write(key, result);
		}
	}
}

