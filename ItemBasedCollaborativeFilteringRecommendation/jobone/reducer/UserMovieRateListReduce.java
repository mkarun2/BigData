package reducer;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*
 * This reducer produces a list of movies and its rate value
 * the particular user has rated
 * The output folder for this job is output1
 */
public class UserMovieRateListReduce extends Reducer<Text, Text, Text, Text> {
	
	private final static Text user_id = new Text();
	private Text movie_rate_list = new Text();
	
	private String formatMovieRateListPerUser(Text key, Iterable<Text> values){
		StringBuilder sb = new StringBuilder();
		sb.append("[");
		for(Text movieRate : values){			
			String rates = movieRate.toString();
			String movieRates[] = rates.split("_");			
			sb.append("("+movieRates[0]+","+movieRates[1]+")-");			
		}
		sb.append("]");
		sb.deleteCharAt(sb.length()-2);	
		return sb.toString();
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String	movieRateListString = formatMovieRateListPerUser(key, values);
		user_id.set(key);
		movie_rate_list.set(movieRateListString);
		context.write(user_id,movie_rate_list);
	} 
	 
	
}