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
//get rid of properties that don't list number of units
else if (field[13].equals("0")){
	context.write(new Text("failed condition3"), new DoubleWritable(count));
}
//make sure price is listed
else if (field[19].contains("$-") || field[19].contains("$0")){
	context.write(new Text("failed condition4"), new DoubleWritable(count));
}
else if (field[10].equals("")){
	context.write(new Text("failed condition5"), new DoubleWritable(count));
}
else if (field[19].equals("")){
	context.write(new Text("failed condition6"), new DoubleWritable(count));
}
else{
	String price =field[19];
	price =price.replaceAll("[$,]", "");
	Double priceDouble=Double.parseDouble(price);
	String units =field[13];
	units =units.replaceAll("[$,]", "");
	Double unitsDouble = Double.parseDouble(units);
	if (priceDouble==0.0 || unitsDouble==0.0){
		context.write(new Text("failed condition7"), new DoubleWritable(count));
	}
	else{
	context.write(new Text(field[10]), new DoubleWritable(priceDouble/unitsDouble));
	}
	
}

}
}