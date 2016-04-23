package hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

public class Context{ //<K extends Text,V extends IntWritable> {


    public static BufferedWriter bufferedWriter;
    public static FileWriter fileWriter;

    public static String foldername;
    public static String instance;

    public void write(Text key, IntWritable value) {

    }

//    public void write(Text key, IntWritable value) {
//        String ip;
//
//        System.out.println("In Map Context write - key: " + key.get() + " value: " + value.get());
//        try {
//            File fdir = new File(foldername);
//            if (!fdir.exists())
//                fdir.mkdirs();
//
//
//            File f = new File(foldername + "/" + key.get());
//
//            if (!f.exists())
//                f.createNewFile();
//
//            System.out.println("list of files in the dir in context: " + fdir.list());
//
//            fileWriter = new FileWriter(f, true);
//            bufferedWriter = new BufferedWriter(fileWriter);
////			bufferedWriter.write(key.get() + "\t" + value.get() + "\n");
//            bufferedWriter.write(value.get() + "\n");
//            bufferedWriter.flush();
//            bufferedWriter.close();
//        } catch (Exception e) {
//            System.out.println("There was a problem fetching the local Host Address!!!");
//        }
//
//
//    }


}
