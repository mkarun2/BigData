package mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/*
 * This mapper takes the input data set 
 * and generates a key with user_id and
 * value as a concatenated string of
 * movie_id and movie_rate
 */
public class UserMovieRateMap extends Mapper<LongWritable, Text, Text, Text> {

	private final static Text userID_key = new Text();
	private static Text movie_rate_value = new Text(); 
	
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException { 

		String line = value.toString().trim();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {

			String strUserID = tokenizer.nextToken().trim();
			String strMovieID = tokenizer.nextToken().trim();
			String strMovieRate = tokenizer.nextToken().trim();
			
			userID_key.set(strUserID);
			movie_rate_value.set(strMovieID+"_"+strMovieRate);
			
			context.write(userID_key, movie_rate_value);
		}
	}
}