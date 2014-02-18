import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Assignment1Mapper2 extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text brand = new Text();

    /**
     * Reads the output from the first job(reducer) and sends to the second reducer
     */
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException {
		System.out.println("reducer2");
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			String strBrand = tokenizer.nextToken();
			if(!strBrand.contains("("))
				continue;
			brand.set(strBrand);
			context.write(brand, one);
			System.out.println("Brand: "+strBrand + "Value: " + one.get());
		}
	}
}