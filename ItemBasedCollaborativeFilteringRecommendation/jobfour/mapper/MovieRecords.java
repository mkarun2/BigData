package mapper;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;

public class MovieRecords extends Mapper<LongWritable, Text, Text, Text>{
	
	private final static Text userID_key = new Text();
	private static Text movie_rate_value = new Text(); 
	
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException { 
		Configuration conf = context.getConfiguration();
		String MID = conf.get("MID");
		
		String line = value.toString().trim();
		StringTokenizer tokenizer = new StringTokenizer(line);

		while (tokenizer.hasMoreTokens()) {
			String strUserID = tokenizer.nextToken().trim();
			String strMovieID = tokenizer.nextToken().trim();			
			if(!strMovieID.equalsIgnoreCase(MID))
				break;
			else{				
				String strMovieRate = tokenizer.nextToken().trim();				
				userID_key.set(strUserID);
				movie_rate_value.set(strMovieID+" "+strMovieRate);				
				context.write(userID_key, movie_rate_value);				
			}		
		}
	}

}
