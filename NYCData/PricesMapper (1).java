// Page Rank Mapper
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PricesMapper
extends Mapper<LongWritable, Text, Text, DoubleWritable> {
@Override
public void map(LongWritable key, Text value, Context context)
throws IOException, InterruptedException {
String line = value.toString();
String[] field = line.split("\\|");
Double count =1.0;

//make sure that it's a valid 
if (field.length !=21){
	String leng = String.valueOf(field.length);
	context.write(new Text("failed condition0"), new DoubleWritable(count));
}
else if (!field[0].contains("1") && !field[0].contains("2") && !field[0].contains("3") && !field[0].contains("4")&& !field[0].contains("5")){
	context.write(new Text("failed condition1"), new DoubleWritable(count));
	//do nothing
}
//get rid of non-residential properties
else if (field[2].contains("05") || field[2].contains("06") || field[2].contains("21") || field[2].contains("22") || field[2].contains("27") || field[2].contains("29") || field[2].contains("30") || field[2].contains("31") || field[2].contains("32") || field[2].contains("33") || field[2].contains("35") || field[2].contains("36") || field[2].contains("37") || field[2].contains("38") || field[2].contains("39") || field[2].contains("40") || field[2].contains("41") || field[2].contains("43") || field[2].contains("44") || field[2].contains("46") || field[2].contains("47")){
	context.write(new Text("failed condition2"), new DoubleWritable(count));
}
//make sure square feet is listed
else if(field[15] == "0"){
	context.write(new Text("failed condition3"), new DoubleWritable(count));
}
//make sure price is listed
else if (field[19].contains("$-") || field[19].contains("$0")){
	context.write(new Text("failed condition4"), new DoubleWritable(count));
}
else if (field[10] ==""){
	context.write(new Text("failed condition5"), new DoubleWritable(count));
}
else if (field[19]==""){
	context.write(new Text("failed condition5"), new DoubleWritable(count));
}
else{
	String sqFeet= field[15];
	String price =field[19];
	sqFeet =sqFeet.replaceAll("[$,]", "");
	price =price.replaceAll("[$,]", "");
	Double sqFeetInt=Double.parseDouble(sqFeet);
	Double priceInt=Double.parseDouble(price);
	Double pricePerFoot=priceInt/sqFeetInt;
	String date = field[20];
	String year = date.replaceAll("[^/]*/[^/]*/", "");
	String yearZip = field[10]+ "|" +year;
	context.write(new Text(yearZip), new DoubleWritable(pricePerFoot));
	//context.write(new Text(price), new DoubleWritable(count));
	
}

}
}