package it.unipi.hadoop;

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

public class Count {
    private static Count singleton = null;

    private Count() {}
    public static Count getCount(){
        if(singleton == null)
            singleton = new Count();
        return singleton;
    }

    public static class CountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable res = new IntWritable();
        private final static Text keyEmit = new Text("Pages");

        private static int count = 0;

        /**
         *
         * In this map phase, we just increase the value of count for each row,
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
            count++;
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            res.set(count);
            context.write(keyEmit, res);
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, LongWritable>{
        private final static Text keyEmit = new Text("Total pages");
        private final static LongWritable pages = new LongWritable();

        private static long count = 0;

        public void reduce(final Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
            for(final IntWritable val : values)
                count += val.get();
            pages.set(count);
            context.write(keyEmit, pages);
        }
    }

    public boolean run(final String input, final String outputDir) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", "-");
        final Job job = Job.getInstance(conf, "count");

        job.setJarByClass(Count.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(CountMapper.class);
        job.setReducerClass(CountReducer.class);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outputDir));

        return job.waitForCompletion(true);
    }

    public int getPageNumber(final String input, final String outputDir) throws Exception{
        if(!Count.getCount().run(input, outputDir))
            return -1;

        //Locates the file in HDFS and retrieves the value of N
        final String filePath = outputDir + "/part-r-0000";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path hdfsPath = new Path(filePath);
        FSDataInputStream inputStream = fs.open(hdfsPath);
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));

        String[] values = reader.readLine().trim().split("-");
        fs.close();
        inputStream.close();
        reader.close();

        return Integer.parseInt(values[1]);
    }

}
