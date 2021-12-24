/* (ii)  The name of all customers who have not placed any order yet.
Select name from customer  where custkey NOT IN (select custkey from order)
*/

package A;
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
import java.util.Arrays;

public class Tasktwo {

    public static final String DELIMITER = "\\|";
    public static void configureJob(Job job, String[] pathsIn, String pathOut) throws IOException, ClassNotFoundException, InterruptedException {

        job.setJarByClass(Tasktwo.class);
        job.setMapperClass(TasktwoMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(TasktwoReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        final Path[] paths = Arrays.copyOf(
                Arrays.stream(pathsIn).map(Path::new).toArray(),
                pathsIn.length,
                Path[].class);

        FileInputFormat.setInputPaths(job, paths);
        FileOutputFormat.setOutputPath(job, new Path(pathOut));

    }



    private static class TasktwoMapper extends Mapper<LongWritable, Text, Text, Text> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

                FileSplit split = (FileSplit) context.getInputSplit();
                String fileName = split.getPath().getName();
                String[] arrayValues = value.toString().split(DELIMITER);
                /*
                Check if the data comes from customer or orders file
                * */
                if(fileName.equalsIgnoreCase("customers.csv")) {
                    String custkey = Utils.getCustomerAttribute(arrayValues, "custkey");
                    String Name = Utils.getCustomerAttribute(arrayValues, "name");
                    context.write(new Text(custkey), new Text("Customer:" + Name));
                }
                else {
                    String custrKey = Utils.getOrdersAttribute(arrayValues, "custkey");
                    context.write(new Text(custrKey), new Text("Orders"));
                }


        }
    }

    public static class TasktwoReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            boolean madeOrder = false;

                String customerName = "";
                for(Text value : values) {
                    String text = value.toString();
                    if(text.startsWith("Customer")) {
                        customerName = text.split(":")[1];
                        continue;
                    }
                    if(text.startsWith("Orders"))
                    {
                        madeOrder = true;
                        break;
                    }
                }
                /*
                If customer does not place an order, output result
                 */
                if(!madeOrder) {
                    context.write(new Text(customerName), new Text(""));
                }

        }
    }





}