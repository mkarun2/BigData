package mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class UserMovieRateMap extends Mapper<LongWritable, Text, Text, Text> {

	private final static Text userID_key = new Text();
	private static Text movie_rate_value = new Text(); 
	
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException { 

		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {

			String strUserID = tokenizer.nextToken();
			String strMovieID = tokenizer.nextToken();
			String strMovieRate = tokenizer.nextToken();
			
			userID_key.set(strUserID);
			movie_rate_value.set(strMovieID+"_"+strMovieRate);
			
			System.out.println("Key: " + userID_key + " Value: " +	movie_rate_value.toString());

			context.write(userID_key, movie_rate_value);
		}
	}
}