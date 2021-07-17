package it.unipi.hadoop;

import it.unipi.hadoop.parser.Parser;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class Count {
    private static Count singleton = null;

    private Count() {}
    public static Count getCount(){
        if(singleton == null)
            singleton = new Count();
        return singleton;
    }

    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private static Text keyEmit = new Text();
        private static Parser parser = new Parser();

        private static String title;
        private static Set<String> outlinks;

        /**
         *
         * In this map phase, we parse line by line the title and the outlinks connected to that page, emitting both
         * (title, 1) and (outlink, 1) for all the outlinks in the list, similarly to what we do for WordCount.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            title = parser.getTitle(value.toString());
            outlinks = parser.getOulinks(value.toString());

            if(title != null) {
                keyEmit.set(title);
                context.write(keyEmit, one);

                if (outlinks != null) {
                    for (String s : outlinks) {
                        keyEmit.set(s);
                        context.write(keyEmit, one);
                    }
                }
            }
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
        private final static Text keyEmit = new Text("Total pages");
        private final static LongWritable pages = new LongWritable();

        private static long count = 0;

        /**
         *
         * In the reduce phase, we get for each title the list of ones that are emitted in the map phase and we increment
         * a static variable count that counts each different page. In this way, we increment count each time we receive a
         * page, even if it is a page that is not in the main list but is just found in the outlinks of another page.
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(final Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            count++;
        }

        /**
         *
         * The cleanup method is called just once at the end of the reducer processing phase and it is used to emit the
         * final value of count we've found, that corresponds to the number of pages inside the starting file. The cleanup
         * method emits a standard text value and the number of pages.
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void cleanup(Context context) throws IOException, InterruptedException {
            pages.set(count);
            context.write(keyEmit, pages);
        }
    }

    public boolean run(final String input, final String outputDir) throws Exception {
        final Configuration conf = new Configuration();

        /*
            Sets the character that separates the key and the value emitted by the reducer, in this case "-"
         */
        conf.set("mapreduce.output.textoutputformat.separator", "-");
        final Job job = Job.getInstance(conf, "count");

        job.setJarByClass(Count.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        return job.waitForCompletion(true);
    }

    public int getPageNumber(final String input, final String outputDir) throws Exception{
        if(!Count.getCount().run(input, outputDir + "/count"))
            return -1;

        //Locates the file in HDFS and retrieves the value of N
        final String filePath = outputDir + "/count/part-r-00000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(filePath);
        FSDataInputStream inputStream = fs.open(hdfsPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String[] values = reader.readLine().trim().split("-");
        inputStream.close();
        reader.close();

        return Integer.parseInt(values[1]);
    }

}
