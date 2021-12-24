package A;

/*
(i)The customer name, address and the average price of orders per customer who have acctbal more than 1000 and for orders placed after 1995-01-01

Select C.name,  C.addresss, avg(price) from Customer C, Order O where C.accbal>1000 and C.custkey=O.custkey and o.orderdate> 1995-01-10 group by C.name
*/


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Taskone {

    public static final String DELIMITER = "\\|";
    public static void configureJob(Job job, String[] pathsIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {

        job.setJarByClass(Taskone.class);
        job.setMapperClass(TaskoneMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TaskoneReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        final Path[] paths = Arrays.copyOf(
                Arrays.stream(pathsIn).map(Path::new).toArray(),
                pathsIn.length, Path[].class);

        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, new Path(pathOut));


    }

    private static class TaskoneMapper extends Mapper<LongWritable, Text, Text, Text> {

        public static LocalDate queryDate = LocalDate.parse("1995-01-01");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                FileSplit split = (FileSplit) context.getInputSplit();
                String fileName = split.getPath().getName();
                String[] arrayValues = value.toString().split(DELIMITER);


                /*
                  Check if the data comes from orders

                 */

                if(fileName.equalsIgnoreCase("orders.csv")) {
                    String dateStr =  Utils.getOrdersAttribute(arrayValues, "orderdate");

                    LocalDate date = parse(dateStr);

                    if(date.isAfter(queryDate) || date.equals(queryDate)) {
                        String custKey = Utils.getOrdersAttribute(arrayValues, "custkey");

                        String price = Utils.getOrdersAttribute(arrayValues, "price");
                        context.write(new Text(custKey), new Text("Orders:" + price));
                    }

                }
                /*
                If the data comes from customer
                */
                else {
                    String custKey = Utils.getCustomerAttribute(arrayValues, "custkey");
                    double acctBal = Double.valueOf(Utils.getCustomerAttribute(arrayValues, "acctbal"));
                    if(acctBal >= 1000) {
                        context.write(new Text(custKey), value);
                    }
                }


        }
    }

    public static LocalDate parse(final String time) {
        return LocalDate.parse(time, DateTimeFormatter.ofPattern("yyyy-MM-dd"));
    }


    public static class TaskoneReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

                double sum = 0;
                int count = 0;
                String custName = "";
                String custAddr = "";

                List<String> customerNames = new ArrayList<>();
                List<String> custAddrs = new ArrayList<>();
                /* If text contain orders, it comes from orders file
                   find sum and counter
                * */
                for(Text value : values) {
                    String text = value.toString();
                    if (text.contains("Orders:")) {
                        double price = Double.valueOf(text.split(":")[1]);
                        sum += price;
                        count++;
                    } else {
                        String[] arrayValues = text.split(DELIMITER);
                        System.out.println(Arrays.toString(arrayValues));
                        custName = arrayValues[1];
                        customerNames.add(custName);

                        custAddr = arrayValues[2];
                        custAddrs.add(custAddr);
                    }
                }
                /*
                 If  counter is greater than zeor and name not null
                 find avg price
                * */
                if(count > 0 && !custName.equals("")) {
                    double avg = sum/count;
                    context.write(new Text(custName + "," + custAddr + "," + avg), new Text(""));

                }


        }
    }


}