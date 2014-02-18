import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Input from Mapper1
 * Text - input key type from mapper
 * Text - input value type from mapper which is Iterable
 * Text - ouput key type of the reducer
 * IntWritable - output value type of the reducer
 */
public class Assignment1Reducer extends Reducer<Text, Text, Text, IntWritable> {
    	
		// Reducer outputs
    	private final static IntWritable one = new IntWritable(1);
    	private Text brandPairs = new Text();
    	
    	/*
    	 * Iterable<Text> A list of all the tsp-brandname for that particular key
    	 * Type is text because we sent the type as text from map
    	 */
		private TreeSet<String> getSortedBrandsOnTimestamp(Text key, Iterable<Text> values) {
			//list to contain each tsp-brand for the particular key eg: 1000 - [ts1-brand1,ts4-brand3]
			List<String> brand_time_list = new ArrayList<String>();
			// for each txt of Type Text in the Iterable <Values>
			for (Text txt : values) {
				//convert from text reference to String because all Text references are same in Hadoop
				//to be easier - Text in Hadoop serves as a pointer to different strings
				String tempStr = txt.toString();
				//adding to list 
				brand_time_list.add(tempStr);
			}
			// sort based on the TimeStamp since TreeSet is a data structure 
			return new TreeSet<String>(brand_time_list);
		}
		
		/**
		 * Method to generate consecutive brand pairs based on the time stamp
		 * @param tset
		 * @return
		 */
		private List<String> getConsecutivePairs(TreeSet<String> tset) {
			//This list os not the efficient way to do, but to just keep it working 
			List<String> list = new ArrayList<String>();
			list.addAll(tset);

			int size = list.size();
			int firstIndex = 0;
			int secondIndex = firstIndex + 1;

			List<String> outputlist = new ArrayList<String>();

			//move the indexes until end of list ie size of list
			while ((firstIndex < size) && (secondIndex < size)) {
				StringBuilder sb = new StringBuilder();
				/*
				 * Remove the timestamp and generate a string with combination of the value pointed to by 
				 * firstIndex and secondIndex in the list.
				 * split - this inbuilt method is used to remove the timestamp from the string
				 */
				sb.append("(" + list.get(+firstIndex).split("-")[1] + "," + list.get(secondIndex).split("-")[1] + ")");
				outputlist.add(sb.toString());
				firstIndex++;
				secondIndex++;
			}
			return outputlist;
		}
    	
		/**
		 * Hadoop knows only this reduce method as this class extends reducer, and this method has to be implemented (Required)
		 * First entry point of the reducer which hadoop searches for
		 */
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	
//        	System.out.println("Reducer Start for key: "+ key);
        	TreeSet<String> tset = getSortedBrandsOnTimestamp(key,values);
//        	System.out.println("TreeSet contents for "+ key + " -> " + tset);
        	List<String> pairList = getConsecutivePairs(tset);
//        	System.out.println("Pari combinations for "+ key + " -> " + pairList);
        	
        	for(String str: pairList){
        		brandPairs.set(str);
        		context.write(brandPairs, one);
        		System.out.println("Key: "+ brandPairs.toString() + " Value: "+ one.get());
        	}        	
        }
    }