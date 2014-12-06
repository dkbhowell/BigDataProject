import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
// import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ZillowZipsProgram {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
        if (args.length != 2){
            System.err.println("Usage: Zillow <input path> <output path>");
            System.exit(-1);
        }
        
        
        Job job = new Job();
        job.setJarByClass(ZillowZipsProgram.class);
        job.setJobName("ZillowZips");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        job.setMapperClass(ZillowZipsMapper.class);
        job.setReducerClass(ZillowZipsReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
}
