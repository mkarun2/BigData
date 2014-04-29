package driver;

import mapper.MovieRecords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CustomerDriver {
	
	public static void main(String[] args) {
		try{
			runJobs(args[0]);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void runJobs(String strCID) throws Exception {

		Configuration conf1 = new Configuration();
		conf1.set("CID", strCID);
		Job job1 = new Job(conf1,"RetrieveCustomerRecords");
		job1.setJarByClass(CustomerDriver.class);
		job1.setMapperClass(MovieRecords.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path("input"));
		FileOutputFormat.setOutputPath(job1, new Path("customers"));
		job1.waitForCompletion(true);
	}
}
