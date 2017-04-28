package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TheReducer extends Reducer<Text,Text,Text,Text>
{
	static ArrayList<Float> theta = new ArrayList<Float>();
	static Float alpha;
	@Override
	protected void setup( Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
	    if (theta.size() == 0 && context.getCacheFiles() != null && context.getCacheFiles().length > 0) 
	    {
	    	alpha = context.getConfiguration().getFloat("alpha", 0);
	    	Path path = new Path(context.getCacheFiles()[0]);
    	    FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(path)));
            String line = "";
            line=br.readLine();
            for(String s: line.split("\t")[1].split(","))
           		theta.add(Float.parseFloat(s));
	    }
	    super.setup(context);
	}
	
	private Float predict(String[] values)
	{		
		Float prediction = (float)0;
		for(int i = 0; i < values.length; i++)
			prediction += Float.parseFloat(values[i]) * theta.get(i);
		
		return prediction;
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{	
		// Load distributed cache file.
		int matchCount = 0;
		ArrayList<Float> errors = new ArrayList<Float>();
		for(int i = 0; i < theta.size(); i++)
			errors.add((float) 0);
		
		for(Text value: values)
		{
			matchCount += 1;
			
			String[] match = value.toString().split(",");
			if(match.length != theta.size())
				throw new IOException("Expected theta (size:" + String.valueOf(theta.size()) +
						") to equal line (size:" + String.valueOf(match.length) + ").");
			Integer target = Integer.parseInt(match[0]);
			match[0] = "1"; // This takes care of the first element being the bias element. 
			
			Float prediction = predict(match);
			for(int i = 0; i < theta.size(); i++)
			{
				errors.set(i, errors.get(i) + ((prediction - target) * Float.parseFloat(match[i])));
			}
		}
		
		for(int i = 0; i < errors.size(); i++)
		{
			theta.set(i, theta.get(i) - alpha * errors.get(i) / matchCount);
		}
		
		
		String s = "";
		for(Float t: theta)
		{
			s += t.toString() + ",";
		}
		context.write(new Text(String.valueOf(matchCount)), new Text(s.substring(0,s.length() - 1)));
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
	}	
}