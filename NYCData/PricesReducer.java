
// Page Rank Reducer
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class PricesReducer
extends Reducer<Text, Text, Text, DoubleWritable> {
public void reduce(Text key, Iterable<DoubleWritable> values,
Context context)
throws IOException, InterruptedException {
	Double total = 1.0;
	Double runningAvg=0.0;
for (DoubleWritable value : values){
	runningAvg = runningAvg*(total-1)/total + value.get()/total;
	total = total +1;	
}
context.write(key, new DoubleWritable(runningAvg));
}
}