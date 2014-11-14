// Page Rank Mapper
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PricesMapper
extends Mapper<LongWritable, Text, Text, DoubleWritable> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
String line = value.toString();
String[] field = line.split(",");

//make sure that it's a valid 
if (field[0] !="1" || field[0] !="2" || field[0] !="3" || field[0] !="4" || field[0] !="5"){
	//do nothing
}
//get rid of non-residential properties
else if (field[2].contains("05") || field[2].contains("06") || field[2].contains("21") || field[2].contains("22") || field[2].contains("27") || field[2].contains("29") || field[2].contains("30") || field[2].contains("31") || field[2].contains("32") || field[2].contains("33") || field[2].contains("35") || field[2].contains("36") || field[2].contains("37") || field[2].contains("38") || field[2].contains("39") || field[2].contains("40") || field[2].contains("41") || field[2].contains("43") || field[2].contains("44") || field[2].contains("46") || field[2].contains("47")){

}
//make sure square feet is listed
else if(field[15] == "0"){
	
}
//make sure price is listed
else if (field[19].contains("$-") || field[19].contains("$0")){
	
}
else if (field[10] ==""){
	
}
else{
	String sqFeet= field[15];
	String price =field[19];
	sqFeet.replaceAll("^[0-9]", "");
	price.replaceAll("^[0-9]", "");
	Double sqFeetInt=Double.parseDouble(sqFeet);
	Double priceInt=Double.parseDouble(price);
	Double pricePerFoot=priceInt/sqFeetInt;
	String date = field[20];
	String year = date.replace("[^/]*/[^/]*/", "");
	String yearZip = field[10]+year;
	context.write(new Text(yearZip), new DoubleWritable(pricePerFoot));
	
}

}
}