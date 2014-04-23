

package driver;
import mapper.FrequentItemSetTwoMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import partitioner.FrequentItemSetTwoPartitioner;
import reducer.FrequentItemSetTwoReducer;
 
public class FrequentItemSetTwoDriver {
 
    public static void main(String[] args) throws Exception {

    	Configuration conf = new Configuration();
 
    	//create a job with configuration
        Job job = new Job(conf, "FrequentItemSet2");
        
        //get the job class
        job.setJarByClass(FrequentItemSetTwoDriver.class);
  
        
        //set the types of the key and value for the final output file
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
 
        // assign the mapper and reducer classes
        job.setMapperClass(FrequentItemSetTwoMapper.class);
        job.setReducerClass(FrequentItemSetTwoReducer.class);
        job.setPartitionerClass(FrequentItemSetTwoPartitioner.class);
         
        // set the format for the output , whether text file or hadoop temp
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        
        // paths for the input and output folders
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
 
        // run the job until completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }        
}