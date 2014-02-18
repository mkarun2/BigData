package mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class FrequentItemSetTwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static Text item_set_key = new Text();
	private IntWritable one = new IntWritable(1);

	private static Map<String, Integer> generate_combinations(String first,	String[] second, Map<String, Integer> hm) {
		if (!first.equals("")) {
			hm.put(first, 1);
		}
		for (int i = 0; i < second.length; i++){
			if (!first.equals(""))
				hm = generate_combinations(first + "," + second[i],	Arrays.copyOfRange(second, i + 1, second.length), hm);
			else
				hm = generate_combinations(first + second[i],Arrays.copyOfRange(second, i + 1, second.length), hm);
		}
		return hm;
	}

	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		Map<String, Integer> hm = new HashMap<String,Integer>();
		String line = value.toString();
		String[] input_itemset = line.split(",");
		hm = generate_combinations("", input_itemset, hm);
		for (Map.Entry<String,Integer> entry : hm.entrySet()) {
			String item_key = entry.getKey();
			item_set_key.set("("+item_key+")");
			context.write(item_set_key, one);
		 }
	}
}