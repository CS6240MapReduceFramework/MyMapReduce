import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Scanner;

public class Evaluation {


	public static void main(String[] args) throws FileNotFoundException {
	
		File pred=new File("../Test/output/part-r-00000");
		File val=new File("all/validate.csv");
		
		double tp=0.0,tn=0.0,fp=0.0,fn=0.0,total=0.0;
		
		Scanner vs=new Scanner(val);
		Scanner ps=new Scanner(pred);
		
		HashMap<String,String> validate = new HashMap<String,String>();
		
		while(vs.hasNext())
		{
			String[] line=vs.nextLine().split(",");
			validate.put(line[0],line[1]);
		}
		vs.close();
		
		while(ps.hasNext())
		{
			String[] line=ps.nextLine().split("\t");
			if(validate.containsKey(line[0]))
			{
				//Predicted delayed
				if(validate.get(line[0]).equals("TRUE"))
				{
					//validation delayed
					if(line[1].equals("0.0"))
						fp++;
					else
						tp++;
					//validation not delayed
				}
				//Predicted not delayed
				else
				{
					//validation delayed
					if(line[1].equals("1.0"))
						fn++;
					else
						tn++;
					//validation not delayed
				}
				total++;
			} else {
				tn++;
				total++;
			}
		}
		ps.close();
		
		
		System.out.println("tp - "+tp+"\nfp - "+fp+"\nfn - "+fn+"\ntn - "+tn+"\ntotal - "+total);
		
		System.out.println("Precision: "+(tp/(tp+fp)));
		System.out.println("Recall:"+(tp/(tp+fn)));
		System.out.println("Accuracy:" +(tp+tn)/total);

	}
}
