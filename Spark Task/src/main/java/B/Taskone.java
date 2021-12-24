package B;

/*
(i)The customer name, address and the average price of orders per customer who haveacctbal more than 1000 and for orders placed after 1995-01-01
Select C.name,  C.addresss, avg(price) from Customer C, Order O where C.accbal>1000 and C.custkey=O.custkey and o.orderdate> 1995-01-10 group by C.name

 */


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.time.LocalDate;


public class Taskone {


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf().setAppName("Spark Join two tables").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> customerRdd = sc.textFile("Data/customers.csv");
        JavaRDD<String> ordersRdd = sc.textFile("Data/orders.csv");

        /*select name,address,customer key where order price>=100*/
        JavaPairRDD<Integer, Tuple2<String, String>> customer_Filtered_RDD = customerRdd.map(s -> {
            String[] customerdetails1 = s.split("\\|");

            return new Tuple4<Integer, String, String, Double>(Integer.parseInt(customerdetails1[0]), customerdetails1[1], customerdetails1[2], Double.parseDouble(customerdetails1[5]));

        }).filter(x -> x._4() > 1000).mapToPair(x -> new Tuple2<>(x._1(), new Tuple2<>(x._2(), x._3())));//price, key, name, address

        //parse date
        LocalDate orderdate = LocalDate.parse("1995-01-01");

        /*select subset of data from orders where date is >=1995.01.01*/

        JavaPairRDD<Integer, Double> order_filtered_RDD = ordersRdd.map(s -> {
            String[] orderdetail = s.split("\\|");
            return new Tuple3<Integer, Double, LocalDate>(Integer.parseInt(orderdetail[1]), Double.parseDouble(orderdetail[3]), LocalDate.parse(orderdetail[4]));
        }).filter(x -> x._3().compareTo(orderdate) > 0).mapToPair(x -> new Tuple2<>(x._1(), x._2()));


        /* join  customer and order RDDs */
        JavaPairRDD<Integer, Tuple4<Double, Integer, String, String>> joined =
                order_filtered_RDD.join(customer_Filtered_RDD).mapToPair(x -> new Tuple2<>(x._1, new Tuple4<>(x._2._1, 1, x._2._2._1, x._2._2._2)));


        /* sum all prices order by a customer */
        JavaPairRDD<Integer, Tuple4<Double, Integer, String, String>> price_Sum =
                joined.reduceByKey((x, y) -> new Tuple4<>(x._1() + y._1(), x._2() + y._2(), x._3(), x._4()));

        /*Customer key, Name, Address and Price */
        JavaRDD<Tuple4<Integer, String, String, Double>> Avg_result =
                price_Sum.map(x -> new Tuple4<>(x._1, x._2._3(), x._2._4(), x._2._1() / x._2._2()));
         //Avg_result.foreach(x -> System.out.println(x.toString()));

        Avg_result.saveAsTextFile("Data/Avg_results.csv");
    }

}

