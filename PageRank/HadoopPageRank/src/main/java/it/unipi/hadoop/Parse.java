package it.unipi.hadoop;


import it.unipi.hadoop.parser.Parser;
import it.unipi.hadoop.pojo.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Parse {
    private static Parse singleton = null;

    private Parse() {}

    public static Parse getParse(){
        if(singleton == null)
            singleton = new Parse();
        return singleton;
    }

    public static class ParseMapper extends Mapper<Object, Text, Text, Text> {
        private static Text keyEmit = new Text();
        private static Text valueEmit = new Text();
        private static Parser parser = new Parser();
        private static final Text placeholder = new Text("");

        private static String title;
        private static List<String> outlinks;

        /**
         *
         * During this map operation, we need to consider that there may be some pages that don't have outlinks and are
         * not listed in the file, but only in the outlinks section. For this reason, for each line we will have to emit
         * both the title of the page and its outlinks and then the outlinks with an empty string as value. The final else
         * in the map phase is used instead for the nodes that do not have any outlinks.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(final Object key, final Text value, Context context) throws IOException, InterruptedException{
            title = parser.getTitle(value.toString());
            outlinks = parser.getOulinks(value.toString());

            if(title != null) {
                keyEmit.set(title);

                if (outlinks != null) {
                    for (String s : outlinks) {
                        valueEmit.set(s);
                        context.write(keyEmit, valueEmit);
                        context.write(valueEmit, placeholder);
                    }
                }
                else{
                    context.write(keyEmit, placeholder);
                }
            }
        }
    }

    public static class ParseReducer extends Reducer<Text, Text, Text, Node> {

        private static Node valueEmit;

        private static int pageNumber;


        /**
         *
         * The setup function is used here for the initialization of pageNumber: in fact, we shouldn't initialize it
         * inside the reduce function for optimization reasons, but we actually could make it global and use a one-time
         * initialization through the setup method. Of course, we can't initialize it outside these methods, since we need
         * the configuration through which the value is passed.
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void setup(Context context) {
            pageNumber = context.getConfiguration().getInt("page.number", 0);
        }

        /**
         *
         * In the reduce phase, we get for each node the list of outlinks, we compute the starting page rank and we emit
         * a node containing those information as value associated to a key equal to the title of the page.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(final Text key, final Iterable<Text> value, Context context) throws IOException, InterruptedException{
            List<String> outlinks = new LinkedList<String>();
            for(Text link : value){
                if(!link.equals("")){
                    outlinks.add(link.toString());
                }
            }
            double rank = 1/pageNumber;
            valueEmit = new Node(rank, outlinks);

            context.write(key, valueEmit);
        }
    }

    public static boolean run(final String input, final String output, final int pageNumber) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "parse");
        conf.set("mapreduce.output.textoutputformat.separator", "\t");

        job.setJarByClass(Parse.class);

        job.setMapperClass(ParseMapper.class);
        job.setReducerClass(ParseReducer.class);

        job.getConfiguration().setInt("page.number", pageNumber);

        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

        return job.waitForCompletion(true);
    }
}
