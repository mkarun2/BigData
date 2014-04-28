package types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class MovieRate implements WritableComparable {
	
	private String movie_name;
	private Double rateValue;
	
	public MovieRate() {
		this("",0.0);
	}
	
	public String getMovie_name() {
		return movie_name;
	}
	public void setMovie_name(String movie_name) {
		this.movie_name = movie_name;
	}
	public Double getRateValue() {
		return rateValue;
	}
	public void setRateValue(Double rateValue) {
		this.rateValue = rateValue;
	}
	
	public MovieRate(String movie_name, Double rateValue) {
		super();
		this.movie_name = movie_name;
		this.rateValue = rateValue;
	}	
	
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.movie_name);
		out.writeDouble(rateValue);
	}
	
	public void readFields(DataInput in) throws IOException {
		this.movie_name = in.readUTF();
		this.rateValue = in.readDouble();
	}
	
	public int hashCode() {
		String movie_name = this.getMovie_name();
		int hash=7;
		for (int i=0; i < movie_name.length(); i++) {
		    hash = hash*31+movie_name.charAt(i);
		}
	    return hash;
	  }
	
	@Override
	public int compareTo(Object other) {
		if(other.getClass().equals("MovieRate")){
			MovieRate tempObj = (MovieRate) other;
		    String movie_name = this.getMovie_name();
		    String other_movie_name = tempObj.getMovie_name();
		    return movie_name.compareTo(other_movie_name);
		}
		return -1;
	}
	
}
