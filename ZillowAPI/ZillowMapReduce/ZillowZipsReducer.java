import java.io.IOException;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

// import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.Text;

public class ZillowZipsReducer extends Reducer<Text, Text, Text, Text>{
    
    private static final Logger logger = Logger.getLogger(ZillowZipsReducer.class.getName());
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String zipCode = key.toString();
        //sum of %*size
        double weighted_sum_size;
        //sum of %*year
        double weighted_sum_years;
        long num_zips = 176;
        //number of size columns that are filled in 
        long num_sizes; 
        //number of years columns that are filled in 
        long num_years;
        double sum_list_prices;

        double average_home_size_per_zip;
        double average_year_built_per_zip;
        double list_price_vs_sale_price; 
        double sale_price_vs_year_built;
        double turnover_vs_1_yr_change; 
        double turnover_vs_value_per_sqft;

        Map<String, Long> categoryCountMap = new HashMap<String, Long>();
        
        for (Text dataText : values){
            String dataString = dataText.toString();
            String[] stringValues = dataString.split(",");
            String neighborhood = stringValues[1].trim()
            String borough = stringValues[2].trim()
            String home_value_index = Double.parseDouble(stringValues[3].trim());
            String median_single_fam_val = Double.parseDouble(stringValues[4].trim());
            String percent_decreasing = Double.parseDouble(stringValues[5].trim());
            String percent_listing_price_reduc = Double.parseDouble(stringValues[6].trim());
            String median_list_per_sqft = Double.parseDouble(stringValues[7].trim());
            String median_list_price = Double.parseDouble(stringValues[8].trim());
            String median_sale_price = Double.parseDouble(stringValues[9].trim());
            String property_tax = Double.parseDouble(stringValues[10].trim());
            String turnover = Double.parseDouble(stringValues[11].trim());
            String median_val_per_sqft = Double.parseDouble(stringValues[12].trim());
            String one_yr_change = Double.parseDouble(stringValues[13].trim());
            String built_2000 = Double.parseDouble(stringValues[14].trim());
            String built_2000 = Double.parseDouble(stringValues[14].trim());
            String built_1900_1919 = Double.parseDouble(stringValues[15].trim());
            String built_1920_1939 = Double.parseDouble(stringValues[16].trim());
            String built_1940_1959 = Double.parseDouble(stringValues[17].trim());
            String built_1960_1979 = Double.parseDouble(stringValues[18].trim());
            String built_1980_1999 = Double.parseDouble(stringValues[19].trim());
            String homesize_1000 = Double.parseDouble(stringValues[20].trim());
            String homesize_3600 = Double.parseDouble(stringValues[21].trim());
            String homesize_1000_1400 = Double.parseDouble(stringValues[22].trim());
            String homesize_1400_1800 = Double.parseDouble(stringValues[23].trim());
            String homesize_2400 = Double.parseDouble(stringValues[24].trim());
            
            //average of all median list prices 
            sumListPrices += median_list_price;

        }
        
        average_year_built_per_zip = getAvgYearBuiltPerZip();
        average_home_size_per_zip = getAvgHomeSizePerZip(homesize_1000, homesize_3600, homesize_1000_1400, homesize_1400_1800, homesize_2400);

        //median sale price vs median list price 
        if(median_list_price != -1 && median_sale_price != -1 ){
            list_price_vs_sale_price = median_sale_price/median_list_price;
        }
        else{
            list_price_vs_sale_price = -1;
        }

        //median sale price vs average year built 
        if(median_sale_price != -1 && average_year_built_per_zip != -1 ){
            sale_price_vs_year_built = median_sale_price/median_list_price;
        }
        else{
            sale_price_vs_year_built = -1;
        }

        //turnover vs 1 yr change 
        if(turnover != -1 && one_yr_change != -1 ){
            turnover_vs_1_yr_change = turnover/one_yr_change;
        }
        else{
            turnover_vs_1_yr_change = -1;
        }

        //turnover vs value per sqft 
        if(turnover != -1 && median_val_per_sqft != -1 ){
            turnover_vs_price_per_sqft = turnover/median_val_per_sqft;
        }
        else{
            turnover_vs_price_per_sqft = -1;
        }
        
        String result = average_year_built_per_zip + ", " + average_home_size_per_zip + ", " + sale_price_vs_year_built + ", " + list_price_vs_sale_price + ", " + turnover_vs_1_yr_change + ", " + turnover_vs_value_per_sqft;
        
        context.write(new Text(zipCode), new Text(result));
    
    }


    private String getAvgHomeSizePerZip(double sz1000, double sz3600, double sz1200, double sz1600, double sz2100) {
            double num_sizes=0;
            if(sz1000 != -1){
                num_sizes++;
            }
            if(sz3600 != -1){
                num_sizes++;
            }
            if(sz2100 != -1){
                num_sizes++;
            }
            if(sz1600 != -1){
                num_sizes++;
            }
            if(sz2100 != -1){
                num_sizes++;
            }

            if (num_sizes > 0){
            double sumPercents = sz1000 + sz3600 + sz1200 + sz1600 + sz2100;
            double avgSize = sumPercents/num_sizes;
            }
            else{
                avg_size = -1;
            }
            return avg_size;
    }


    private String getAvgYearBuiltPerZip(double s, double yr1900, double yr1930, double yr1950, double yr1970, double yr1990) {
            double num_years=0;
            if(yr2000 != -1){
                num_years++;
            }
            if(yr1900 != -1){
                num_years++;
            }
            if(yr1930 != -1){
                num_years++;
            }
            if(yr1950 != -1){
                num_years++;
            }
            if(yr1970 != -1){
                num_years++;
            }
            if(yr1990 != -1){
                num_years++;
            }

            if(num_years > 0){
            double sumPercents = yr2000+ yr1900+ yr1930+ yr1950+ yr1970+ yr1990;
            double avg_yr = sumPercents/num_years;
            }
            else{
                avg_yr = -1;
            }

            return avg_yr;
    }
    
}
