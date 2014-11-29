// import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class YelpMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = Logger.getLogger(YelpMapper.class
            .getName());

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (isHeader(line)) {
            return;
        }
        StringTokenizer tokenizer = new StringTokenizer(line, ",");

        String name = tokenizer.nextToken();
        String rating = tokenizer.nextToken();
        String numReviews = tokenizer.nextToken();
        String categories = tokenizer.nextToken();
        String neighborhoods = tokenizer.nextToken();
        String zipCode = tokenizer.nextToken();
        String seperator = ", ";

        String result = rating + seperator + numReviews + seperator + categories;
        zipCode = zipCode + seperator;
        
        Text zipText = new Text(zipCode);
        Text resultText = new Text(result);
        
        context.write(zipText, resultText);
        

        //logValues(zipCode, numReviews);
        //writeToContext(zipCode, tuple, context);
    }

    private boolean isHeader(String line) {
        String[] headers = { "Name", "Rating", "Reviews", "Categories" };
        for (int i = 0; i < headers.length; i++) {
            if (!line.contains(headers[i])) {
                return false;
            }
        }
        return true;
    }

    private String getRating(StringTokenizer tokenizer) {
        if (tokenizer.hasMoreTokens()) {
            tokenizer.nextToken();
        } else {
            throw new IllegalStateException("tokenizer out of tokens");
        }
        if (tokenizer.hasMoreTokens()) {
            String stringValue = tokenizer.nextToken().trim();
            double doubleValue = Double.parseDouble(stringValue);
            return stringValue;
        } else {
            throw new IllegalStateException("tokenizer out of tokens");
        }
    }

    private String getNumReviews(StringTokenizer tokenizer) {
        String stringValue;
        if (tokenizer.hasMoreTokens()) {
            stringValue = tokenizer.nextToken().trim();
        } else {
            throw new IllegalStateException("tokenizer out of tokens");
        }
        long longValue = Long.parseLong(stringValue);
        return stringValue;
    }

    private String getZip(StringTokenizer tokenizer) {
        for (int i = 0; i < 2; i++) {
            if (tokenizer.hasMoreTokens()) {
                tokenizer.nextToken();
            } else {
                throw new IllegalStateException("tokenizer out of tokens");
            }
        }
        if (tokenizer.hasMoreTokens()) {
            return tokenizer.nextToken().trim();
        } else {
            throw new IllegalStateException("tokenizer out of tokens");
        }
    }

    private void logValues(Text zip, LongWritable numReviews) {
        logger.log(Level.INFO, "Zipcode: " + zip.toString() + ", NumReviews: "
                + numReviews.toString());
    }
}
