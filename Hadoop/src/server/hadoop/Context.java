package hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;

public class Context {


    private static BufferedWriter bufferedWriter;
    private static FileWriter fileWriter;


    public void write(String key, Integer value) throws IOException, Exception {
        String ip = InetAddress.getLocalHost().getHostAddress();

        File f = new File(ip + "/" + key);
        if (!f.exists())
            f.createNewFile();

        fileWriter = new FileWriter(key, true);
        bufferedWriter = new BufferedWriter(fileWriter);
        bufferedWriter.write(key + "\t" + value + "\n");
        bufferedWriter.flush();
    }
}
