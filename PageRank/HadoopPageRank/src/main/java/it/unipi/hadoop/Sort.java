package it.unipi.hadoop;

import it.unipi.hadoop.hadoopobjects.Node;
import it.unipi.hadoop.hadoopobjects.Page;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;


import java.io.IOException;

public class Sort {

    private static Sort singleton = null;

    private Sort() { }

    public static Sort getSort()
    {
        if (singleton == null)
            singleton = new Sort();

        return singleton;
    }


    public static class SortMapper extends Mapper<Text, Text, Page, NullWritable> {
        private static final Node node = new Node();
        private static final Page keyEmit = new Page();
        private static final NullWritable nullValue = NullWritable.get();

        /**
         *
         * The map phase is very simple, since we just take the information about each page, in particular its title and
         * its pageRank, and we use a WritableComparable object called Page to automatically sort the pages exploiting the
         * Shuffle and Sort phase of MapReduce. Thanks to that, we just need to emit the WritableComparable objects as keys
         * and the reducer will receive the pages to be processed already ordered.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void map(final Text key, final Text value, final Context context) throws IOException, InterruptedException {
            node.setByJson(value.toString());
            keyEmit.set(key.toString(), node.getPageRank());
            context.write(keyEmit, nullValue);
        }
    }


    public static class SortReducer extends Reducer<Page, NullWritable, Text, DoubleWritable> {
        private static final Text title = new Text();
        private static final DoubleWritable pageRank = new DoubleWritable();

        /**
         *
         * The reduce function of the sort phase just takes the pages and emit their title and their rank.
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void reduce(final Page key, final Iterable<NullWritable> values, final Context context) throws IOException, InterruptedException {
            title.set(key.getTitle());
            pageRank.set(key.getPageRank());
            context.write(title, pageRank);
        }
    }


    public boolean run(final String input, final String outputDir) throws Exception {
        final Configuration conf = new Configuration();
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        final Job job = new Job(conf, "Sort");
        job.setJarByClass(Sort.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(Page.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outputDir + "/sort"));

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        return job.waitForCompletion(true);
    }

}
