package it.unipi.hadoop;

import it.unipi.hadoop.hadoopobjects.Node;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class Rank {
    private static Rank singleton;

    private Rank() {}

    public static Rank getRank(){
        if(singleton == null)
            singleton = new Rank();
        return singleton;
    }

    public static class RankMapper extends Mapper<Object, Text, Text, Node>{
        private static final Text keyEmit = new Text();
        private static final Node nodeEmit = new Node();

        private static double mass;

        /**
         *
         * In this map phase, we first of all retrieve the node information from the input value, using Json to standardize
         * and facilitate the retrieval of data. Then, we emit the title of the page and the node information, in particular
         * we will emit a node object in which we're only interested about the outlinks (we emit the whole node because this
         * allows to emit in the same mapper both a list of Strings and an integer value corresponding to the mass later on).
         * After that, we calculate the contribution of the node for each outlink and we send that value on each outlink:
         * in this reducer, we will understand how to handle the two different semantics for Node thanks to the isNode boolean
         * field of the Node object.
         *
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(final Object key, final Text value, Context context) throws IOException, InterruptedException{
            keyEmit.set(key.toString());
            if(value.toString().startsWith("\"")){
                System.out.println(value.toString());
            }
            nodeEmit.setByJson(value.toString());
            context.write(keyEmit, nodeEmit);

            mass = nodeEmit.getPageRank()/nodeEmit.getAdjacencyList().size();

            nodeEmit.setIsNode(false);
            nodeEmit.setAdjacencyList(new LinkedList<String>());
            for(String outlink : nodeEmit.getAdjacencyList()){
                keyEmit.set(outlink);
                nodeEmit.setPageRank(mass);
                context.write(keyEmit, nodeEmit);
            }
        }
    }

    public static class RankReducer extends Reducer<Text, Node, Text, Node>{
        private static final Node nodeEmit = new Node();
        private static double alpha;
        private static int pageNumber;

        private static double rank;

        /**
         *
         * As in the parse reducer, we use the setup method to get the value of alpha and the number of pages.
         *
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void setup(Context context) throws IOException, InterruptedException{
            alpha = context.getConfiguration().getDouble("alpha", 0.0);
            pageNumber = context.getConfiguration().getInt("page.number", 0);
        }

        /**
         *
         * In the reduce method, we start using a rank equal to zero. From the list of nodes we obtained for that page,
         * we check node by node if it corresponds to a contribution through an outlink (case isNode = false) or if it's
         * the propagation of the structure (isNode = true): in the first case, we add the contribution to the rank; in the
         * second case, we recreate the structure of the node. After analyzing the whole list, we set the new page rank
         * according to the given formula: note that, in case a node doesn't have any incoming links, the list values will
         * only contain a single Node that is the one used for the propagation of the structure. That node will be assigned
         * a newPageRank equal to alpha/pageNumber, that is a default value for dangling nodes. Finally, we emit the title
         * and the updated node information to be used in the following iteration.
         *
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(final Text key, final Iterable<Node> values, Context context) throws IOException, InterruptedException{
            rank = 0.0;

            for(Node n : values){
                if(!n.getIsNode())
                    rank += n.getPageRank();
                else
                    nodeEmit.set(n);
            }

            nodeEmit.setPageRank( (alpha / ((double)pageNumber)) + ((1 - alpha) * rank) );
            context.write(key, nodeEmit);
        }
    }

    public static boolean run(final String input, final String outputDir, final double alpha, final int pageNumber, final int iteration) throws Exception {
        final Configuration conf = new Configuration();
        final Job job = Job.getInstance(conf, "rank-" + iteration);

        /*
            Here we will have to specify which is the separator used in each line of the input file, so that we can
            divide in the map phase the key and the value.
         */
        conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");

        job.setJarByClass(Rank.class);

        job.setMapperClass(RankMapper.class);
        job.setReducerClass(RankReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Node.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Node.class);

        job.getConfiguration().setDouble("alpha", alpha);
        job.getConfiguration().setInt("page.number", pageNumber);

        job.setNumReduceTasks(5);

        /*
            Here we use the KeyValueTextInputFormat, that is an extended version of TextInputFormat, which is useful for us
            since each record is formed by a Text/Text pair to be splitted in key and value.
         */
        KeyValueTextInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(outputDir + "/rank-" + iteration));

        return job.waitForCompletion(true);
    }
}
