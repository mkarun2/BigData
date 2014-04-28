package mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import types.MovieRate;

public class MoviePairRatesValueMap extends Mapper<LongWritable, Text, Text, Text> {
	
	public void map(LongWritable key, Text value, Context context)	throws IOException, InterruptedException { 
		Map<String,List<MovieRate>> userMovieRateListHM = new HashMap<String,List<MovieRate>>();
		Set<String> moviePairs = new TreeSet<String>();
		
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);

		List<MovieRate> tempList = new ArrayList<MovieRate>();

		while (tokenizer.hasMoreTokens()) {			
			createMovieObjects(tokenizer,userMovieRateListHM,tempList);
			createContextPairs(userMovieRateListHM,moviePairs);
			writeToContext(moviePairs,context);			
		}	
	}
	
	private void writeToContext(Set<String> moviePairs,Context context) throws IOException, InterruptedException{
		for(String str : moviePairs){
			String tmp[] = str.split(" ");
			if(tmp != null && tmp.length >= 2)
				context.write(new Text(tmp[0]),new Text(tmp[1]));	
		}
	}
	
	private void createContextPairs(Map<String,List<MovieRate>> userMovieRateListHM, Set<String> moviePairs){
		for(Map.Entry<String,List<MovieRate>> e:  userMovieRateListHM.entrySet()){
			List<MovieRate> tmpList = e.getValue();
			if(tmpList != null){
				int j = 0;
				StringBuilder sb1 = new StringBuilder();	
				StringBuilder sb2 = new StringBuilder();
				for(int i = 0; i < tmpList.size() - 1; i++){
					j = i + 1;
					while(j < tmpList.size()){
						sb1.append("(");
						sb1.append(tmpList.get(i).getMovie_name()+","+tmpList.get(j).getMovie_name());
						sb1.append(")");
						sb2.append("(");
						sb2.append(tmpList.get(i).getRateValue()+","+tmpList.get(j).getRateValue());
						sb2.append(")");
						j++;
					}	
				}
				String pair = sb1.toString() +" "+sb2.toString();
				if(!moviePairs.contains(pair))
					moviePairs.add(pair);
			}
		}
	}
	
	private void createMovieObjects(StringTokenizer tokenizer,
			Map<String,List<MovieRate>> userMovieRateListHM,
			List<MovieRate> tempList){
		
		String strUserID = tokenizer.nextToken();
		String strMovieRateList = tokenizer.nextToken();
		
		//remove extra characters
		strMovieRateList = strMovieRateList.replaceAll("[\\[()\\]]", "").trim();
		
		//get individual ratings
		String[] ratings = strMovieRateList.split("-");
		for(String str : ratings){
			String[] movieRateSplit = str.split(",");
			MovieRate movRat = new MovieRate();
			movRat.setMovie_name(movieRateSplit[0].trim());
			movRat.setRateValue(Double.parseDouble(movieRateSplit[1].trim()));
			tempList.add(movRat);
		}
		
		// add movie rate objects
		userMovieRateListHM.put(strUserID, tempList);
	}
	
	@SuppressWarnings("unused")
	private void print(Map<String,List<MovieRate>> userMovieRateListHM){
		System.out.println("Total count: "+ userMovieRateListHM.size() );
		for(Map.Entry<String, List<MovieRate>> e : userMovieRateListHM.entrySet()){
			System.out.println(e.getKey());
			for(MovieRate mr : e.getValue()){
				System.out.println(mr.getMovie_name() + "-->" + mr.getRateValue());
			}
			System.out.println();
		}
	}
}