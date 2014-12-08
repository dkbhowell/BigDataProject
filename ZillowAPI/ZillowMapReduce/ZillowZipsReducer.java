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
        double sum_list_prices=0;

        double average_home_size_per_zip;
        double average_year_built_per_zip;
        double list_price_vs_sale_price; 
        double sale_price_vs_year_built;
        double homesize_val_per_sqft_vs_list_price;
        double zillow_index_vs_median_list_price;
        double median_list_sqft_vs_median_val_sqft;
//        double turnover_vs_1_yr_change; 
//        double turnover_vs_value_per_sqft;
        
        for (Text dataText : values){
            String dataString = dataText.toString();
            String[] stringValues = dataString.split(",");
            String neighborhood = stringValues[1].trim();
            String borough = stringValues[2].trim();
            double home_value_index = Double.parseDouble(stringValues[3].trim());
            double median_single_fam_val = Double.parseDouble(stringValues[4].trim());
            double percent_decreasing = Double.parseDouble(stringValues[5].trim());
            double percent_listing_price_reduc = Double.parseDouble(stringValues[6].trim());
            double median_list_per_sqft = Double.parseDouble(stringValues[7].trim());
            double median_list_price = Double.parseDouble(stringValues[8].trim());
            double median_sale_price = Double.parseDouble(stringValues[9].trim());
            double property_tax = Double.parseDouble(stringValues[10].trim());
            double turnover = Double.parseDouble(stringValues[11].trim());
            double median_val_per_sqft = Double.parseDouble(stringValues[12].trim());
            double one_yr_change = Double.parseDouble(stringValues[13].trim());
            double built_1900 = Double.parseDouble(stringValues[14].trim());
            double built_2000 = Double.parseDouble(stringValues[15].trim());
            double built_1900_1919 = Double.parseDouble(stringValues[16].trim());
            double built_1920_1939 = Double.parseDouble(stringValues[17].trim());
            double built_1940_1959 = Double.parseDouble(stringValues[18].trim());
            double built_1960_1979 = Double.parseDouble(stringValues[19].trim());
            double built_1980_1999 = Double.parseDouble(stringValues[20].trim());
            double homesize_1000 = Double.parseDouble(stringValues[21].trim());
            double homesize_3600 = Double.parseDouble(stringValues[22].trim());
            double homesize_1000_1400 = Double.parseDouble(stringValues[23].trim());
            double homesize_1400_1800 = Double.parseDouble(stringValues[24].trim());
            double homesize_1800_2400 = Double.parseDouble(stringValues[25].trim());
            
            //average of all median list prices 
            sum_list_prices += median_list_price;
            
            //average year built 
            average_year_built_per_zip = getAvgYearBuiltPerZip(built_1900, built_2000, built_1900_1919, built_1920_1939, built_1940_1959, built_1960_1979, built_1980_1999 );
            
            //average home size per zipcode 
            average_home_size_per_zip = getAvgHomeSizePerZip(homesize_1000, homesize_3600, homesize_1000_1400, homesize_1400_1800, homesize_1800_2400);

            //median sale price vs median list price 
	        if(median_list_price != 0 && median_sale_price != 0 ){
	            list_price_vs_sale_price = median_sale_price/median_list_price;
	        }
	        else{
	            list_price_vs_sale_price = 0;
	        }
	
	        //median sale price vs average year built 
	        if(median_sale_price != 0 && average_year_built_per_zip != 0 ){
	            sale_price_vs_year_built = median_sale_price/average_year_built_per_zip;
	        }
	        else{
	            sale_price_vs_year_built = 0;
	        }
	        
	        //homesize*value per sqft vs median list price 
	        if( average_home_size_per_zip != 0 && median_val_per_sqft != 0 && median_list_price != 0 ){
	            homesize_val_per_sqft_vs_list_price = (average_home_size_per_zip*median_val_per_sqft)/median_list_price;
	        }
	        else{
	        	homesize_val_per_sqft_vs_list_price  = 0;
	        }
	
	        //zillow price vs median list price 
	        if( home_value_index != 0 && median_list_price != 0 ){
	            zillow_index_vs_median_list_price = home_value_index/median_list_price;
	        }
	        else{
	        	 zillow_index_vs_median_list_price = 0;
	        }
	        
	        
	        //zillow price vs median list price 
	        if(median_list_per_sqft != 0 && median_val_per_sqft  != 0 ){
	        	median_list_sqft_vs_median_val_sqft = median_list_per_sqft/median_val_per_sqft ;
	        }
	        else{
	        	median_list_sqft_vs_median_val_sqft = 0;
	        }
	        
//			NOT A GOOD METRIC        
//	        //turnover vs 1 yr change 
//	        if(turnover != 0 && one_yr_change != 0 ){
//	            turnover_vs_1_yr_change = turnover/one_yr_change;
//	        }
//	        else{
//	            turnover_vs_1_yr_change = -1;
//	        }
	
//			NOT A GOOD METRIC	        
//	        //turnover vs value per sqft 
//	        if(turnover != 0 && median_val_per_sqft != 0 ){
//	        	turnover_vs_value_per_sqft = turnover/median_val_per_sqft;
//	        }
//	        else{
//	        	turnover_vs_value_per_sqft = 0;
//	        }

	        String result = average_year_built_per_zip + ", " + average_home_size_per_zip + ", " + sale_price_vs_year_built + ", " + list_price_vs_sale_price + ", " +  homesize_val_per_sqft_vs_list_price + ", " +  zillow_index_vs_median_list_price + ", " +  median_list_sqft_vs_median_val_sqft;
	        
	        context.write(new Text(zipCode), new Text(result));
        }
    }

    private double getAvgHomeSizePerZip(double sz1000, double sz3600, double sz1200, double sz1600, double sz2100) {
            double num_sizes=0;
            double avg_size=-1;
            double factor1000=0;
            double factor3600=0;
            double factor1200=0;
            double factor1600=0;
            double factor2100=0;
            if(sz1000 != -1){
                num_sizes++;
                factor1000=1000*sz1000;
            }
            if(sz3600 != -1){
                num_sizes++;
                factor3600=3600*sz3600;
            }
            if(sz1200 != -1){
                num_sizes++;
                factor1200=1200*sz1200;
            }
            if(sz1600 != -1){
                num_sizes++;
                factor1600=1600*sz1600;
            }
            if(sz2100 != -1){
                num_sizes++;
                factor2100= 2100*sz2100;
            }

            if (num_sizes > 0){
            avg_size =  factor1000 + factor3600 + factor1200 + factor1600 + factor2100;
            }
            return avg_size;
    }


    private double getAvgYearBuiltPerZip(double yr1900, double yr2000, double yr1910, double yr1930, double yr1950, double yr1970, double yr1990) {
            double num_years=0;
            double avg_yr = -1;
            double factor1900=0;
            double factor2000=0;
            double factor1910=0;
            double factor1930=0;
            double factor1950=0;
            double factor1970=0;
            double factor1990=0;
            if(yr1900 != -1){
                num_years++;
                factor1900=1890*yr1900;
            }
            if(yr2000 != -1){
                num_years++;
                factor2000 = 2000*yr2000;
            }
            if(yr1910 != -1){
                num_years++;
                factor1910 =1910*yr1910;
            }
            if(yr1930 != -1){
                num_years++;
                factor1930 = 1930*yr1930;
            }
            if(yr1950 != -1){
                num_years++;
                factor1950=1950*yr1950;
            }
            if(yr1970 != -1){
                num_years++;
                factor1970=1970*yr1970;
            }
            if(yr1990 != -1){
                num_years++;
                factor1990= 1990*yr1990;
            }

            if(num_years > 0){
            System.out.println( factor1900 + factor2000 + factor1910 + factor1930 + factor1950 + factor1970 + factor1990);
            avg_yr= factor1900 + factor2000 + factor1910 + factor1930 + factor1950 + factor1970 + factor1990;
            }
            return avg_yr;
    }
    
}
