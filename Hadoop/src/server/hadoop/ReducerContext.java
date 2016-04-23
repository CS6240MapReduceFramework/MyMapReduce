package hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Created by ummehabibashaik on 4/22/16.
 */
public class ReducerContext extends Context {// <K extends Text, V extends IntWritable> extends Context {


    public void write(Text key, IntWritable value) {
        String ip;

        System.out.println("In reduce Context write - key: " + key.get() + " value: " + value.get());
        try {
            File fdir = new File(foldername);
            if (!fdir.exists())
                fdir.mkdirs();

            File f = new File(foldername + "/part-" + instance);

            if (!f.exists())
                f.createNewFile();

            fileWriter = new FileWriter(f, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            bufferedWriter.write(key.get() + "\t" + value.get() + "\n");
            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (Exception e) {
            System.out.println("There was a problem fetching the local Host Address!!!");
        }


    }
}
