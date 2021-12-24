package A;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MainLocal extends Configured implements Tool {

    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "MapReduce Task");

        if(args[0].equals("-taskone")) {
            Taskone.configureJob(job, new String[]{args[1], args[2]}, args[3]);
        }
        else if(args[0].equalsIgnoreCase("-tasktwo")) {

            Tasktwo.configureJob(job, new String[]{args[1], args[2]}, args[3]);
        }
        boolean success = job.waitForCompletion(true);
        return success ? 0 : 1;
    }


    public static void main(String[] args) throws Exception {
        MainLocal driver = new MainLocal();
        int exitCode = ToolRunner.run(driver,args);
        System.exit(exitCode);

    }

}