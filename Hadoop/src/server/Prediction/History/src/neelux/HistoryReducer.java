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

public class HistoryReducer extends Reducer<Text,Text,Text,Text> {
	public int getHour(String deptime) {
		String hours="0000"+deptime;
		return Integer.parseInt(hours.substring(hours.length()-4,hours.length()-2));
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
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

		Instances trainInstances = new Instances("Train",attributes,0);
		
		for(Text val : values){
			String[] valArray = val.toString().split(",");
			
			Instance inst = new Instance(trainInstances.numAttributes());
			inst.setDataset(trainInstances);
			try{
				inst.setValue(0, Integer.parseInt(valArray[1]));
				inst.setValue(1, Integer.parseInt(valArray[2]));
				inst.setValue(2, valArray[3].hashCode());
				inst.setValue(3, Integer.parseInt(valArray[4]));
				inst.setValue(4, Integer.parseInt(valArray[5]));
				inst.setValue(5, getHour(valArray[6]));
				inst.setValue(6, Integer.parseInt(valArray[7]));
				if(Double.parseDouble(valArray[0]) > 0.0) {
					inst.setValue(7, 1);
				} else {
					inst.setValue(7, 0);
				}
				trainInstances.add(inst);
			} catch(Exception e){
				System.out.println(e+"\n......................."+key.toString());
				continue;
			}
		}
		trainInstances.setClassIndex(trainInstances.numAttributes()-1);
		RandomForest rf = new RandomForest();
		rf.setNumTrees(10);
		try
		{
			rf.buildClassifier(trainInstances);
			weka.core.SerializationHelper.write("forests/"+key.toString()+".model", rf);
		} catch (Exception e) {
			System.out.println(e+"\n---------------------"+key.toString());
		}
	}
}

