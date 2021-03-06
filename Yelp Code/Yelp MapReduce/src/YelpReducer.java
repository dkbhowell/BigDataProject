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

public class YelpReducer extends Reducer<Text, Text, Text, Text>{
    
    private static final Logger logger = Logger.getLogger(YelpReducer.class.getName());
    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
        String zipCode = key.toString();
        long sumReviews = 0;
        double sumBizRatings = 0;
        long numBiz = 0;
        long sumRatings = 0;
        double averageReview;
        double averageBusinessReview;
        double reviewsPerBiz;
        Map<String, Long> categoryCountMap = new HashMap<String, Long>();
        
        for (Text dataText : values){
            String dataString = dataText.toString();
            String[] stringValues = dataString.split(",");
            double rating = Double.parseDouble(stringValues[0].trim());
            long numRatings = Long.parseLong(stringValues[1].trim());
            String categories = stringValues[2].replace("[", "").replace("]", "");
            logger.log(Level.INFO, categories);
            
            sumBizRatings += rating;
            sumReviews += numRatings;
            numBiz++;
            sumRatings += rating*numRatings;
            
            countCategories(categories, categoryCountMap);
        }
        
        averageReview = (double) sumRatings / sumReviews;
        averageBusinessReview = sumBizRatings / numBiz;
        reviewsPerBiz = (double) sumReviews / numBiz;
        String mostCommonCat = getMostCommonCat(categoryCountMap);
        String secondMostCommonCat = getNthMostCommonCat(categoryCountMap, 2);
        String thirdMostCommonCat = getNthMostCommonCat(categoryCountMap, 3);
        
        averageBusinessReview = (double) Math.round(averageBusinessReview*10)/10;
        averageReview = (double) Math.round(averageReview*10)/10;
        reviewsPerBiz = (double) Math.round(reviewsPerBiz*10)/10;
        
        String result = numBiz + ", " + sumReviews + ", " + reviewsPerBiz + ", " + averageReview + ", " + averageBusinessReview + ", " +
                mostCommonCat + ", " + secondMostCommonCat + ", " + thirdMostCommonCat;
        
        context.write(new Text(zipCode), new Text(result));
    }
    
    private String getMostCommonCat(Map<String, Long> countMap) {
        long max = 0;
        String maxKey = "";
        for (Entry<String, Long> entry : countMap.entrySet()){
            if (entry.getValue() > max){
                max = entry.getValue();
                maxKey = entry.getKey();
            }
        }
        return maxKey + ", " + max;
    }
    
    private String getNthMostCommonCat(Map<String, Long> countMap, int n) {
        long max = 0;
        String maxKey = "";
        List<Entry<String,Long>> maxEntries = new LinkedList<Entry<String,Long>>();
        for (Entry<String, Long> entry : countMap.entrySet()){
            if (maxEntries.size() < n){
                addInOrder(entry, maxEntries);
            }else{
                if (entry.getValue() > maxEntries.get(n-1).getValue()){
                    maxEntries.remove(n-1);
                    addInOrder(entry, maxEntries);
                }
            }
        }
        if (maxEntries.size() < (n)){
            return "n/a, n/a";
        }else{
            return maxEntries.get(n-1).getKey() + ", " + maxEntries.get(n-1).getValue();
        }
    }
    
    private void addInOrder(Entry<String,Long> entry, List<Entry<String,Long>> list){
        if (list.isEmpty()){
            list.add(entry);
            return;
        }
        int index = 0;
        for (Entry<String,Long> listEntry : list){
            if (listEntry.getValue() < entry.getValue()){
                list.add(index, entry);
                return;
            }
            index++;
        }
        list.add(entry);
    }

    private void countCategories(String categoryString, Map<String,Long> countMap){
        logger.log(Level.INFO, categoryString);
        String[] categoryArray = categoryString.split("\\|");
        for (String category : categoryArray){
            category = category.trim();
            if (countMap.containsKey(category)){
                Long count = countMap.get(category);
                countMap.put(category, new Long(count.longValue() + 1));
                // count++;
                logger.log(Level.INFO, "increasing count of " + category + " to " + count);
            }else{
                countMap.put(category, new Long(1));
                logger.log(Level.INFO, "new entry, putting a fresh count for " + category);
            }
        }
    }
}
