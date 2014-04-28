package driver;

import mapper.MoviePairRatesValueMap;
import mapper.UserMovieRateMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import reducer.SimilarityReducer;
import reducer.UserMovieRateListReduce;


public class RecommenderDriver {

	public static void main(String[] args) {
		
		try{
			runJobs();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	private static void runJobs() throws Exception { 
		
		Configuration conf1 = new Configuration();
		Job job1 = new Job(conf1, "ItemBasedCollaborativeFilteringRecommenderJob1");
		job1.setJarByClass(RecommenderDriver.class);
        job1.setMapperClass(UserMovieRateMap.class);
        job1.setReducerClass(UserMovieRateListReduce.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
		Job job2 = new Job(conf2, "ItemBasedCollaborativeFilteringRecommenderJob2");
		job2.setJarByClass(RecommenderDriver.class);
        job2.setMapperClass(MoviePairRatesValueMap.class);
        job2.setReducerClass(SimilarityReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path("output1"));
        FileOutputFormat.setOutputPath(job2, new Path("output2"));
        job2.waitForCompletion(true);
        
	}

}
