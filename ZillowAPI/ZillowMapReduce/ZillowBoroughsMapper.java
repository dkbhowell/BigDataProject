// import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;

import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class ZillowBoroughsMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static final Logger logger = Logger.getLogger(ZillowZipsMapper.class
            .getName());

    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line = value.toString();
        if (isHeader(line)) {
            return;
        }
        StringTokenizer tokenizer = new StringTokenizer(line, ",");

        String zipCode = tokenizer.nextToken();
        String neighborhood = tokenizer.nextToken();
        String borough = tokenizer.nextToken();
        String home_value_index = tokenizer.nextToken();
        String median_single_fam_val = tokenizer.nextToken();
        String percent_decreasing = tokenizer.nextToken();
        String percent_listing_price_reduc = tokenizer.nextToken();
        String median_list_per_sqft = tokenizer.nextToken();
        String median_list_price = tokenizer.nextToken();
        String median_sale_price = tokenizer.nextToken();

        String property_tax = tokenizer.nextToken();
        String turnover = tokenizer.nextToken();
        String median_val_per_sqft = tokenizer.nextToken();
        String one_yr_change = tokenizer.nextToken();
        String built_1900 = tokenizer.nextToken();
        String built_2000 = tokenizer.nextToken();
        String built_1900_1919 = tokenizer.nextToken();
        String built_1920_1939 = tokenizer.nextToken();
        String built_1940_1959 = tokenizer.nextToken();
        String built_1960_1979 = tokenizer.nextToken();
        String built_1980_1999 = tokenizer.nextToken();

        String homesize_1000 = tokenizer.nextToken();
        String homesize_3600 = tokenizer.nextToken();
        String homesize_1000_1400 = tokenizer.nextToken();
        String homesize_1400_1800 = tokenizer.nextToken();
        String homesize_2400 = tokenizer.nextToken();

        String seperator = ", ";

        String result = zipCode + seperator +neighborhood + seperator + borough + separator + home_value_index + seperator + median_single_fam_val + seperator + percent_decreasing + seperator + percent_listing_price_reduc + seperator + median_list_per_sqft + seperator + median_list_price + seperator + median_sale_price + seperator +  property_tax + seperator + turnover + seperator + median_val_per_sqft + seperator + one_yr_change + seperator + built_2000 + seperator + built_1900_1919 + seperator + built_1920_1939 + seperator + built_1940_1959 + seperator + built_1960_1979 + seperator + built_1980_1999 + seperator +  homesize_1000 + seperator + homesize_3600 + seperator + homesize_1000_1400 + seperator + homesize_1400_1800 + seperator + homesize_2400;
        borough = borough + seperator;
        
        Text zipText = new Text(zipCode);
        Text resultText = new Text(result);
        System.out.println(zipText + " : " + resultText)
        context.write(zipText, resultText);
        
    }

    private boolean isHeader(String line) {
        String[] headers = { "Zip", "Neighborhood", "Borough", "Zillow Home Value Index (USD)","Median Single Family Home Value (USD)",
        "Percent Homes Decreasing(%)", "Percent Listing Price Reduction(%)", "Median List Price Per Sq Ft(USD)",
         "Median List Price (USD)", "Median Sale Price (USD)" , "Property Tax (USD)" , "Turnover (Sold Within Last Yr.) (%)" , "Median Value Per Sq Ft (USD)" , "1-Yr. Change (USD),BuiltYear<1900 (%)" ,"BuiltYear>2000 (%)" , "BuiltYear1900-1919 (%)" , "BuiltYear1920-1939 (%)" , "BuiltYear1940-1959 (%)" , "BuiltYear1960-1979 (%)" , "BuiltYear1980-1999 (%)" , "HomeSize<1000sqft (%)" , "HomeSize>3600sqft (%)" , "HomeSize1000-1400sqft (%)", "HomeSize1400-1800sqft (%)", "HomeSize1800-2400sqft (%)" };
        for (int i = 0; i < headers.length; i+ seperator ++) {
            if (!line.contains(headers[i])) {
                return false;
            }
        }
        return true;
    }

}
