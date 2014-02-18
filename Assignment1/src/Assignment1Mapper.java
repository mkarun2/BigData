import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * LongWritable - Input key type
 * Text - Input Value type
 * Text - output key type
 * Text - output value type
 */
public class Assignment1Mapper extends Mapper<LongWritable, Text, Text, Text> {

	private final static Text userID_key = new Text();
	private Text brands_time_values = new Text();

	/** 
	 * LongWritable key -> line number of the input files
	 * Text value -> line
	 * Context -> Hadoop internal file system
	 */
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		//read each line from the input files present in the input folder
		String line = value.toString();
		
		//StringTokenizer is a Java class under Strings which helps to output each word from the line
		StringTokenizer tokenizer = new StringTokenizer(line);
		
		//loop through the tokens - hasMoreTokens checks if a token is present
		while (tokenizer.hasMoreTokens()) {
			
			// get the user id,brand name, time stamp  - nextToken() gives the next word in the line
			String strUserID = tokenizer.nextToken();
			String strBrandName = tokenizer.nextToken();
			String strTimestamp = tokenizer.nextToken();
			
			// I was getting special characters in the input, so in order to check if the userid is a number, this is a regex
			strUserID = strUserID.replaceAll("[^\\d.]", "");
			
			//type is Text which is a Hadoop data type, so set String because context accepts only Hadoop types
			userID_key.set(strUserID);
			
			//logical part: concatenate time stamp with brand name eg: ts1-brand2
			String concatTimeBrand = strTimestamp + "-" + strBrandName;
			
			// setting into Hadoop type
			brands_time_values.set(concatTimeBrand);
			
//			System.out.println("Key: " + userID_key + " Value: " + brands_time_values);
			
			//writing into hadoop context which will be read into the reducers
			context.write(userID_key, brands_time_values);
		}
	}
}