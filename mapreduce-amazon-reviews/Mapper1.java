package stubs;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	    
	String line = value.toString();
    String[] arr = line.split("\t");
    
    int customer_id = 1;
    int product_id = 3;
    int star_rating = 7;
    int minRating = 4; 
    
    //Omit header row using byte offset
    if (key.get() == 0 && line.contains("marketplace")) {
    	return;
    } else {
    	//Map users who gave a rating of at least 4 to a product
    	if (Integer.parseInt(arr[star_rating]) >= minRating) {
    		context.write(new Text(arr[product_id]), new Text(arr[customer_id]));
    	}
    }
  }
}
