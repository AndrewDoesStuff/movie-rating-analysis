package project;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class MLReducer extends Reducer<Text, DoubleWritable, NullWritable, Put> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
  @Override
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
			throws IOException, InterruptedException {
	  
	  double avgScore = 0;
	  int total = 0;
	  double curScore;
	  
	  for (DoubleWritable score:values){
		  curScore = score.get();
		  avgScore += (double)curScore;
		  total += 1;
	  }
	  
	  avgScore = avgScore / total;
	  
	  String avg = String.valueOf(avgScore);
	  
	  String genre = key.toString();
	  Put put = new Put(Bytes.toBytes(genre));
	  byte[] col = Bytes.toBytes("Score");
	  byte[] qual = Bytes.toBytes("Average");
	  byte[] scoreVal = Bytes.toBytes(avg);
	  
	  put.addImmutable(col, qual, scoreVal);
	  context.write(null, put);
	  
	}
}