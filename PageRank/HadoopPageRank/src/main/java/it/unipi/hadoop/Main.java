package it.unipi.hadoop;

import org.apache.hadoop.conf.Configuration;

import java.sql.Timestamp;

public class Main {
    public static void main(String[] args) throws Exception{
        if(args.length != 2){
            System.out.println("Error: wrong number of arguments. Please insert in order the number of iterations\n" +
                    "and the value for alpha");
            System.exit(1);
        }

        final Configuration conf = new Configuration();
        final String INPUT_PATH = "PageRank/HadoopPageRank/src/main/resources/wiki-micro.txt";
        final String OUTPUTS_PATH = "PageRank/HadoopPageRank/src/main/resources/outputs";
        final int NUM_ITER = Integer.parseInt(args[0]);
        final double ALPHA = Double.parseDouble(args[1]);

        final Timestamp startingTime = new Timestamp(System.currentTimeMillis());

        /**
         *
         * Count phase: in this phase, we count how many pages are present in the file saved in the INPUT_PATH variable.
         * The value is saved inside HDFS after the first computation and will be retrieved only once from the Main.
         * In particular, we call the function getPageNumber() that will start the MapReduce-based count, instantiating the
         * singleton for the Count class.
         *
         */
        int pageNumber = Count.getCount().getPageNumber(INPUT_PATH, OUTPUTS_PATH);
        if(pageNumber < 1){
            System.out.println("Count job failed or there are no pages to be processed!");
            System.exit(1);
        }

        final Timestamp afterCount = new Timestamp(System.currentTimeMillis());
        System.out.println("Count phase ended after " + ((afterCount.getTime() - startingTime.getTime())/1000) + " seconds");

        /**
         *
         * Parsing and node creation phase: this phase must be placed after the previous one since it exploits pageNumber
         * to initialize the rank of each node. In the map phase, for each line of the input file we obtain the title and
         * the list of outlinks, emitting not only a kv pair as (title, outlinks) but also a set of (outlinks, ) for all
         * the nodes that are not listed in the file. In the reduce phase, for each title we create a node with the starting
         * page rank and the list of outlinks.
         *
         */
        if(!Parse.getParse().run(INPUT_PATH, OUTPUTS_PATH, pageNumber)){
            System.out.println("An error occurred during the execution of the parsing and node creation phase");
            System.exit(1);
        }

        final Timestamp afterParse = new Timestamp(System.currentTimeMillis());
        System.out.println("Parse and node creation phase ended after " + ((afterParse.getTime() - afterCount.getTime())/1000) + " seconds");

    }
}
