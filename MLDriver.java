package project;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;

public class MLDriver extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    
    int exitCode = ToolRunner.run(new Configuration(), new MLDriver(), args);
    System.exit(exitCode);

  }

  public int run(String[] args) throws Exception {

    /*
     * Checks for proper arguments and returns correct usage.
     */
    if (args.length != 2) {
      System.out.printf(
          "Usage: MLDriver <input dir> <output dir>\n");
      return -1;
    }

    /*
     * Instantiate a Job object for your job's configuration.  
     */
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "MovieLens");
    
    /*
     * Specify the jar file that contains your driver, mapper, and reducer.
     * Hadoop will transfer this jar file to nodes in your cluster running
     * mapper and reducer tasks.
     */
    job.setJarByClass(MLDriver.class);
    job.setJobName("Movie Rating Analyis");
    
    /*
     * Input data will be read via file path from command line inputs.
     * This job uses TableOutputFormat rather than a FileOutput, so there is no need
     * to specify FileOutputFormat.
     */
    FileInputFormat.setInputPaths(job, new Path(args[0]));

    /*
     * Specify the mapper and reducer classes.
     */
    job.setMapperClass(MLMapper.class);
    job.setReducerClass(MLReducer.class);

    /*
     * setOutputFormatClass needs to be called since we are using a table
     * output rather than the default TextOutputFormat.
     */
    job.setOutputFormatClass(TableOutputFormat.class);

    /*
     * setMapOutputKeyClass and setMapOutputValueClass must be called
     * since the output classes for the mapper will be different from
     * the reducer.
     * The mapper will output a (Text, DoubleWritable) pair, while the
     * reducer will output to an HBase table.
     */
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(DoubleWritable.class);

    /*
     * Output table name is specified using command line arguments.
     */
    job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, args[1]);

    /*
     * Start the MapReduce job and wait for it to finish.
     * If it finishes successfully, return 0. If not, return 1.
     */
    boolean success = job.waitForCompletion(true);
    return success ? 0 : 1;
  }
}
