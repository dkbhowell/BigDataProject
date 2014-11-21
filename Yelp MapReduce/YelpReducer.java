import java.io.IOException;
import java.util.logging.Logger;

// import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class YelpReducer extends Reducer<Text, Text, Text, Text>{
    
    private static final Logger logger = Logger.getLogger(YelpReducer.class.getName());
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String zipCode = key.toString();
        long sumReviews = 0;
        double sumRatings = 0;
        long numBusinesses = 0;
        double averageReview;
        double averageBusinessReview;
        
        for (Text tuple : values){
            String tupleString = tuple.toString();
            String[] stringValues = tupleString.split(",");
            double rating = Double.parseDouble(stringValues[0].trim());
            long numRatings = Long.parseLong(stringValues[1].trim());
            sumRatings += rating;
            sumReviews += numRatings;
            numBusinesses++;
        }
        
        averageReview = sumRatings / sumReviews;
        averageBusinessReview = sumRatings / numBusinesses;
        
        String result = sumReviews + ", " + averageReview + ", " + averageBusinessReview;
        
        context.write(new Text(zipCode), new Text(result));
    }
}
