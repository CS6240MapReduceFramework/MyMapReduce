package wordcount;

import hadoop.*;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.StringTokenizer;
import java.util.*;


public class WordCount {

    public static class WordCountMapper extends Mapper<Text, Text, Text, IntWritable> {
        //TODO: Use Text and IntWritable
        private Text word = new Text();
        private IntWritable one = new IntWritable(1);

        //TODO: Throw error if the given data types doesn't match with the Mapper data types
        public void map(Object key, Text value, Context context) throws IOException {

            //System.out.println("inside map method...");
            String input = value.get().replaceAll("[^a-zA-Z0-9 ]", "");

            StringTokenizer tokens = new StringTokenizer(input, " ");
            while (tokens.hasMoreTokens()) {
                word.set(tokens.nextToken().trim());
                context.write(word, one);
            }
        }
    }

    public static class WordCountRedcuer extends Reducer<Text, CustomIterable, Text, IntWritable> {
        //TODO: Throw error if the data types doesn't match
        public void reduce(Text key, CustomIterable<IntWritable> values, Context context) throws Exception {
            IntWritable sum = new IntWritable(0);

            while (values.hasNext()) {
                int temp = values.next().get();
                System.out.println("Value - " + temp);
                sum.set(sum.get() + temp);
            }
            context.write(key, sum);
        }

    }


    public static void main(String args[]) {
        Job job = null;
        try {
            Configuration conf = new Configuration();

            job = Job.getInstance(conf, "Word Count");

            job.setJarByClass(WordCount.class);
            job.setMapperClass(WordCountMapper.class);
            job.setReducerClass(WordCountRedcuer.class);
            //TODO: Implement setOutput
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(1);
            // FileInputFormat.addInputPath(job, args[0]);
            // FileOutputFormat.setOutputPath(job,args[1]);

            job.waitForCompletion(true);

            System.out.println(job.getJobname() + " job completed successfully!");
        } catch (Exception e) {
            System.out.println(job.getJobname() + " job failed!!");
            e.printStackTrace();
        }
    }
}
