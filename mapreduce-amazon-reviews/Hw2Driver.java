package stubs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;


public class Hw2Driver {

  public static void main(String[] args) throws Exception {

    /*
     * Validate that two arguments were passed from the command line.
     */
    if (args.length != 3) {
      System.out.printf("Usage: Hw2Driver <input dir> <temp output dir> <final output dir>\n");
      System.exit(-1);
    }
    
    Configuration conf = new Configuration();
    Path temp = new Path(args[1]);
    
    //Set up job1
    Job job1 = Job.getInstance(conf, "job1");
    job1.setJarByClass(Hw2Driver.class);
    job1.setMapperClass(Mapper1.class);
    job1.setReducerClass(Reducer1.class);
    
    FileInputFormat.setInputPaths(job1, new Path(args[0]));
    FileOutputFormat.setOutputPath(job1, temp);
    
    job1.setMapOutputKeyClass(Text.class);
    job1.setMapOutputValueClass(Text.class);
    job1.setOutputKeyClass(UserPairWritable.class);
    job1.setOutputValueClass(Text.class);
    
    //Wait for job2 to complete
    int flag = job1.waitForCompletion(true)?0 : 1;
    if (flag != 0) {
    	System.out.println("Job1 failed, exiting");
    	System.exit(flag);
    }
    //Set up job2
    Job job2 = Job.getInstance(conf, "job2");
    job2.setJarByClass(Hw2Driver.class);
    job2.setMapperClass(Mapper2.class);
    job2.setReducerClass(Reducer2.class);
    
    FileInputFormat.setInputPaths(job2, temp);
    FileOutputFormat.setOutputPath(job2, new Path(args[2]));
    
    job2.setMapOutputKeyClass(UserPairWritable.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(UserPairWritable.class);
    job2.setOutputValueClass(Text.class);
    
    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    System.exit(job2.waitForCompletion(true) ? 0 : 1);
  }
}

