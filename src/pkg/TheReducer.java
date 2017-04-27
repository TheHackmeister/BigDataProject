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
            
            //for(String s: line.split(","))
            //  	theta.add(Float.parseFloat(s));
	    }
	    super.setup(context);
	}
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		
		// Load distibuted cache file. 
		for(Text value: values)
		{
			context.write(key, value);
			// Split and process. 
		}
		
		//context.write(new Text(""),  new Text(""));
	}
}