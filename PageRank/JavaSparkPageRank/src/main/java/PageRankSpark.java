import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class PageRankSpark {

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Error: wrong number of arguments. Please insert in order the path of the input file,\n" +
                    "the path for the outputs, the number of iterations and the value for alpha");
            System.exit(1);
        }

        final String INPUT_PATH = args[0];
        final String OUTPUT_PATH = args[1];
        final int NUM_ITER = Integer.parseInt(args[2]);
        final double ALPHA = Double.parseDouble(args[3]);

        final SparkConf sparkConf = new SparkConf().setMaster("yarn").setAppName("PageRank");
        final JavaSparkContext sc = new JavaSparkContext(sparkConf);

        final JavaRDD<String> pages = sc.textFile(INPUT_PATH).cache();

        /**
         *
         * Parse phase: for the parse phase, we first of all create an RDD in which we have a list of tuples with title
         * and list of outlinks for the corresponding page: at the same time, we insert in the list also all the pages
         * that we can find in the outlinks, associated with an empty list for outlinks. Since we may have replication,
         * we perform a reduceByKey to merge possible duplicates in the list with all the nodes.
         *
         *
         */
        JavaPairRDD<String, Iterable<String>> all_nodes = pages.flatMapToPair(new PairFlatMapFunction<String, String, Iterable<String>>() {
            public Iterator<Tuple2<String, Iterable<String>>> call(String page) throws Exception {
                String[] arr;
                String title = "";
                Matcher matcher;
                List<String> outlinks = new LinkedList<String>();
                List<Tuple2<String, Iterable<String>>> myRDD = new ArrayList<Tuple2<String, Iterable<String>>>();
                while (page != null) {
                    Pattern pattern = Pattern.compile("<title.*?>(.*?)</title>");
                    matcher = pattern.matcher(page);

                    if (matcher.find()) {
                        title = matcher.group(1).replace("\t", "");
                    }
                    arr = page.split("\\[\\[");
                    for (int i = 0; i < arr.length; i++) {
                        if (arr[i].contains("]]") && !outlinks.contains(arr[i].substring(0, arr[i].indexOf("]]")))) {
                            outlinks.add(arr[i].substring(0, arr[i].indexOf("]]")));
                            myRDD.add(new Tuple2<String, Iterable<String>>(arr[i].substring(0, arr[i].indexOf("]]")), new ArrayList<String>()));
                        }
                    }
                    myRDD.add(new Tuple2<String, Iterable<String>>(title, outlinks));
                }
                return myRDD.iterator();
            }
        });

        JavaPairRDD<String, Iterable<String>> unique_nodes = all_nodes.reduceByKey(new Function2<Iterable<String>, Iterable<String>, Iterable<String>>() {
            public Iterable<String> call(Iterable<String> outlinks1, Iterable<String> outlinks2) throws Exception {
                List<String>  ret = new ArrayList<String>();

                for(String link : outlinks1){
                    ret.add(link);
                }

                for(String link : outlinks2){
                    ret.add(link);
                }

                return ret;
            }
        }).cache();

        /**
         *
         * Count phase: we count the number of tuples in the previously generated RDD.
         *
         */

        final long pageNumber = unique_nodes.count();

        /*
            Getting the list of titles to which we match the initial mass. This is then joined with the unique_nodes RDD to
            have a single RDD to work from.
        */
        JavaRDD<String> nodes_titles = unique_nodes.keys().cache();

        JavaPairRDD<String, Double> titles_and_masses = nodes_titles.mapToPair(new PairFunction<String, String, Double>() {

            public Tuple2<String, Double> call(String title) throws Exception {
                return new Tuple2<String, Double>(title, 1.0/pageNumber);
            }
        });

        /**
         *
         * Ranking phase: in the ranking phase, we start by creating an RDD with all the nodes with outlinks and ranks inside,
         * taking the last computed value of the rank from the RDD titles_and_masses. After this, we compute a new RDD
         * where for each node we output all the outlinks connected to the contribution that node has for that outlink
         * (we don't emit anything if the node has no outlinks). We then reduce by key to obtain the total value converged
         * to each node: finally, we compute a new RDD that is passed in the following iteration that contains the rank of
         * each node that must be used in the following step.
         *
         */

        for(int i = 0; i < NUM_ITER; i++){

            JavaPairRDD<String, Tuple2<Iterable<String>, Double>> complete_nodes = unique_nodes.join(titles_and_masses);

            final JavaRDD<Tuple2<Iterable<String>, Double>> outlink_nodes_with_mass = complete_nodes.values();

            JavaPairRDD<String, Double> outlink_masses = outlink_nodes_with_mass.flatMapToPair(new PairFlatMapFunction<Tuple2<Iterable<String>, Double>, String, Double>() {
                public Iterator<Tuple2<String, Double>> call(Tuple2<Iterable<String>, Double> myTuple) throws Exception {

                    List<Tuple2<String, Double>> ret = new ArrayList<Tuple2<String, Double>>();
                    Double node_mass = myTuple._2;
                    List<String> outlinks_list = new ArrayList<String>();
                    for(String outlink : myTuple._1){
                        outlinks_list.add(outlink);
                    }

                    if(outlinks_list.isEmpty())
                        return ret.iterator();
                    else{
                        for(String pageToOutput : outlinks_list)
                            ret.add(new Tuple2<String, Double>(pageToOutput, node_mass/outlinks_list.size()));
                    }
                    return ret.iterator();
                }
            });

            JavaPairRDD<String, Double> outlink_total_values = outlink_masses.reduceByKey(new Function2<Double, Double, Double>() {
                public Double call(Double contribution1, Double contribution2){
                    return contribution1 + contribution2;
                }
            });

            titles_and_masses = outlink_total_values.mapValues(new Function<Double, Double>() {
                public Double call(Double nodeTotalValue) throws Exception {
                    return ALPHA/pageNumber + (1-ALPHA)* nodeTotalValue;
                }
            });
        }

        /**
         *
         * Sorting phase: in this phase, we take the last RDD that we generated in the rank phase with the list of titles
         * and ranks and we sort it based on the ranks.
         *
         */

        JavaPairRDD<String, Double> result = titles_and_masses.mapToPair(x -> x.swap()).sortByKey().mapToPair(x -> x.swap());
        result.saveAsTextFile(OUTPUT_PATH);

    }
}