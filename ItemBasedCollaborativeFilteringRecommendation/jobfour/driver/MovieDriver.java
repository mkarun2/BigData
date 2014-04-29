package driver;

import mapper.MovieRecords;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieDriver {
	
	public static void main(String[] args) {
		try{
			runJobs(args[0]);
		}catch(Exception e){
			e.printStackTrace();
		}
	}

	private static void runJobs(String strMID) throws Exception {

		Configuration conf1 = new Configuration();
		conf1.set("MID", strMID);
		Job job1 = new Job(conf1,"RetrieveMovieRecords");
		job1.setJarByClass(MovieDriver.class);
		job1.setMapperClass(MovieRecords.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path("input"));
		FileOutputFormat.setOutputPath(job1, new Path("movies"));
		job1.waitForCompletion(true);
	}
}
