package stubs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, Text, UserPairWritable, Text> {

  @Override
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  
	  //Insert iterable values into an ArrayList
	  List<String> valuesList = new ArrayList<>();
	  for (Text value : values) {
		  valuesList.add(value.toString());
	  }
	  //Sort ArrayList
	  Collections.sort(valuesList);
	  
	  //Remove duplicates in ArrayList
	  List<String> newList = new ArrayList<>();
	  
	  for (String element : valuesList) {
		  if (!newList.contains(element)) {
			  newList.add(element);
		  }
	  }
	  //u1	u2?
	  //u2	u1?
	  //Create pairs of users who gave a rating to a common product 
	  for (int i = 0; i < newList.size(); i++) {
		  for (int j = i + 1; j < newList.size(); j++) {
			  context.write(new UserPairWritable(newList.get(i), newList.get(j)), key);
		  }
	  }
  }
}