package B;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

public class Tasktwo {

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Spark Single  Source Shortest Path").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String sourceNode = "17274";//set source node
       /* Read file */
        JavaRDD<String> lines = sc.textFile("Data/ca-GrQc.txt");
        /*create nodes and assign costs */
        JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> nodes = lines.flatMap(x -> Arrays.stream(x.split("\\t")).iterator()).distinct().mapToPair(x -> {
            ArrayList<String> path = new ArrayList<>();
            /*
             17274 | destination-node-id | path | cost: output structure
             */
            if (x.equals(sourceNode)) {
                return new Tuple2<>(x, new Tuple2<>(path, 1));
            }
            return new Tuple2<>(x, new Tuple2<>(path, Integer.MAX_VALUE));//(Dest Node, path, cost)
        });

        /*
        creating edges  and the structure
         */
        JavaPairRDD<String, String> edges = lines.mapToPair(x -> {
            String[] split = x.split("\\t");
            return new Tuple2<>(split[0], split[1]);
        });

        /*
        set source nodes and in each iteration the source changes
        */

        JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> nextIteration = nodes.filter(x -> x._1().equals(sourceNode));//filter source and activate it
        /*
        loop until no more nodes are activate
        */
        while (nextIteration.count() > 0) {

            /* send messages */
            JavaPairRDD<String, Tuple2<ArrayList<String>, Integer>> messages = nextIteration.join(edges).mapToPair(x -> {
                String destination = x._2._2;
                ArrayList<String> path = new ArrayList<>(x._2._1._1);
                path.add(x._1);
                return new Tuple2<>(destination, new Tuple2<>(path, path.size()));
            }).reduceByKey((x, y) -> {
                if (x._2() < y._2()) {
                    return x;
                } else {
                    return y;
                }
            });

            /*
            check node local state and messages
            */
            nextIteration = messages.join(nodes).flatMapToPair(x -> {
                Tuple2<ArrayList<String>, Integer> message = x._2._1;
                Tuple2<ArrayList<String>, Integer> local = x._2._2;
                if (message._2 < local._2) {
                    return Collections.singleton(new Tuple2<>(x._1, message)).iterator();
                }
                return Collections.<Tuple2<String, Tuple2<ArrayList<String>, Integer>>>emptyIterator();
            });
            /*
             nodes and joins and return the least cost
             */
            nodes = nodes.union(nextIteration).reduceByKey((x, y) -> {
                if (x._2 < y._2) {
                    return x;
                } else {
                    return y;
                }
            });
        }
        nodes.collect().forEach(x -> System.out.println(x._1 + "|" + String.join(",", x._2._1) + " | " + x._2._2));
        nodes.saveAsTextFile("Data/nodes.csv");
    }
}