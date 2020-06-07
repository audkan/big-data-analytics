package stubs;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer2 extends Reducer<UserPairWritable, Text, UserPairWritable, Text> {

  @Override
  public void reduce(UserPairWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  int count = 0;
	  int minCommonProducts = 3;
	  
	  //Initialize StringBuilder and Text to aggregate all products of the same pair of users
	  StringBuilder products = new StringBuilder();
	  Text results = new Text();
	  
	  //Delimiter formatting
	  boolean first = true;
	  for (Text value : values) {
		  //Keep count for at least 3 common products
		  count++; 
		  if (first) {
			  first = false;
		  } else {
			  products.append(",");
		  }
		  //Append each product to the final result
		  products.append(value.toString());
	  }
	  
	  results.set(products.toString());
	  
	  //Outputs the pair of users if there are at least 3 common products 
	  if (count >= minCommonProducts) {
		  context.write(key, new Text(results));
	  }
  }
}