import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
 
public class FacebookBrandPairing {
 
    public static void main(String[] args) throws Exception {
        
    	// job1 starts
    	//Configuration for job1
    	Configuration conf = new Configuration();
 
    	//create a job with configuration
        Job job = new Job(conf, "FacebookBrandPairing");
        
        //get the job class
        job.setJarByClass(FacebookBrandPairing.class);
        
        //set the types of the key and value for the final output file
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        // assign the mapper and reducer classes
        job.setMapperClass(Assignment1Mapper.class);
        job.setReducerClass(Assignment1Reducer.class);
         
        // set the format for the output , whether text file or hadoop temp
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        // paths for the input and output folders
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
        // run the job until completion
        job.waitForCompletion(true);
        
        //job2 starts
        Configuration conf2 = new Configuration();
        
        Job job2 = new Job(conf2, "FacebookBrandPairing");
        job2.setJarByClass(FacebookBrandPairing.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
 
        job2.setMapperClass(Assignment1Mapper2.class);
        job2.setReducerClass(Assignment1Reducer2.class);
         
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.addInputPath(job2, new Path("output"));
        FileOutputFormat.setOutputPath(job2, new Path("output2"));
 
        job2.waitForCompletion(true);
    }        
}