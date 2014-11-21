
// Page Rank Reducer
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
public class PricesReducer
extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
@Override
public void reduce(Text key, Iterable<DoubleWritable> values,
Context context)
throws IOException, InterruptedException {
Double newAvg = 0.0;
Double total = 1.0;
for (DoubleWritable value : values) {
newAvg = newAvg*(total-1)/total + value.get()/total;
total++;
}
context.write(key, new DoubleWritable(newAvg));
}
}