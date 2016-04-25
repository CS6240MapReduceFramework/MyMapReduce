package hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

public class Context<T> {


    public static BufferedWriter bufferedWriter;
    public static FileWriter fileWriter;

    public static String foldername;
    public static String instance;
    public static String phase;


    
    /**
     * Writes the given key value pairs in specified folder name
     * @param key - A key 
     * @param value - A value
     */
    public void write(T key, T value) {

        try {
            File fdir = new File(foldername);
            if (!fdir.exists())
                fdir.mkdirs();

            String filename = "";

            if (phase.equals("MAPPER"))
                filename = foldername + "/" + key.toString();
            else if (phase.equals("REDUCER"))
                filename = foldername + "/part-" + instance;
            else {
                System.out.println("PHASE should be set to either MAPPER or REDUCER!");
                System.exit(1);
            }

            File f = new File(filename);

            if (!f.exists())
                f.createNewFile();

            fileWriter = new FileWriter(f, true);
            bufferedWriter = new BufferedWriter(fileWriter);
            if (phase.equals("MAPPER"))
                bufferedWriter.write(value.toString() + "\n");
            else if (phase.equals("REDUCER"))
                bufferedWriter.write(key.toString() + "\t" + value.toString() + "\n");

            bufferedWriter.flush();
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();        }

    }


}
