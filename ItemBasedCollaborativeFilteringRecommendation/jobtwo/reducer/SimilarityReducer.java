package reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SimilarityReducer extends Reducer<Text, Text, Text, Text>{
	PearsonsCorrelation p = new PearsonsCorrelation();
	
	@SuppressWarnings("unused")
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		List<String> tmplist = new ArrayList<String>();
		int count = 0;
		for(Text txt : values){
			String s = txt.toString();
			count++;
			tmplist.add(s);
		}
		
		if (count >= 2) {
			double[] v1 = new double[count];
			double[] v2 = new double[count];
			double dbSimilarity = 0.0;
			int i = 0;
			for (String str : tmplist) {
				str = str.replaceAll("[()]", "").trim();
				//System.out.println("Values: " + str);
				String vectorsStr[] = str.split(",");
				if (vectorsStr != null && vectorsStr.length >= 2) {
					v1[i] = Double.parseDouble(vectorsStr[0]); // x values
					v2[i] = Double.parseDouble(vectorsStr[1]); // y values
					i++;
				}
			}
			dbSimilarity = p.correlation(v1,v2);
			//dbSimilarity = CosineSimilarity.cosineSimilarity(v1,v2);
		if(!Double.isNaN(dbSimilarity))
			context.write(key, new Text(String.valueOf(dbSimilarity)));
		}
	}

}
