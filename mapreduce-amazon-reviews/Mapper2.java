package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper2 extends Mapper<LongWritable, Text, UserPairWritable, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String line = value.toString();
	  String[] arr = line.split("\t");
	  
	  //Identity Mapper
	  context.write(new UserPairWritable(arr[0], arr[1]), new Text(arr[2])); 
  }
}
