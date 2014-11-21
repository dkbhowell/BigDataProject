// calculating the page rank
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class Prices {
public static void main(String[] args) throws Exception {
	if (args.length != 2) {
		System.err.println("Usage: MaxTemperature <input path> <output path>");
		System.exit(-1);
		}
	@SuppressWarnings("deprecation")
	Job job = new Job();
	setTextoutputformatSeparator(job, " ");  
	job.setJarByClass(Prices.class);
	job.setJobName("Page Rank");
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	job.setMapperClass(PricesMapper.class);
	job.setReducerClass(PricesReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(DoubleWritable.class);
	job.waitForCompletion(true);
	}



//code for removing the default tab in the output file is taken from here:
//https://github.com/jasebell/SpringXDTransformerDemo/blob/master/src/co/uk/dataissexy/xd/mapreduce/TwitterHashtagJob.java
static void setTextoutputformatSeparator(final Job job, final String separator){
final Configuration conf = job.getConfiguration(); //ensure accurate config ref
conf.set("mapred.textoutputformat.separator", separator); //Prior to Hadoop 2 (YARN)
conf.set("mapreduce.textoutputformat.separator", separator); //Hadoop v2+ (YARN)
conf.set("mapreduce.output.textoutputformat.separator", separator);
conf.set("mapreduce.output.key.field.separator", separator);
conf.set("mapred.textoutputformat.separatorText", separator); // ?
}
}