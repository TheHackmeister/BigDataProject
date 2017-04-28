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
            String count = "";
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
		// Load distibuted cache file.
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
		//context.write(new Text(""),  );
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		
		String line = "169622	-0.5,0.006912429,0.008442301,-0.06695476,0.016619306,0.005639009,2.0634115E-4,-0.12960878,-0.01487425,0.003101013,0.0026323237,0.006650081,0.007961821,-0.0055269953,0.007490184,0.009014161,0.017987054,0.0055269953,0.008395137,-0.014838877,0.004492342,0.018921485,0.027629081,0.0055122566,0.010912499,0.016970087,0.0106413085,0.004106189,-0.01121022,0.01413437,0.004356746,1.2085697E-4,0.002505571,0.0031599675,0.009046586,-0.07653783,0.00146797,0.0113959275,0.007192463,0.007106979,0.041123793,0.016203677,0.0035166428,0.047381826,0.0012380469,0.0059868414,0.0045395056,0.007843912,0.053536687,0.019976772,0.009683296,-0.23115811,0.0049551358,0.009229345,0.018726934,0.009317777,0.018924432,0.033610027,0.005639009,0.009441582,0.035929892,0.009647923,0.0154637955,0.0066766106,0.002290387,6.9271674E-4,0.005037672,0.008819611,0.0014208063,0.0035785453,0.026388086,0.004076712,0.07848923,8.047305E-4,0.0028740375,0.0035755974,0.007381118,-6.396576E-4,0.0105381375,-0.05621028,-0.07426808,0.0061548618,-0.0036610817,0.01353598,0.012760727,0.007381118,0.003230713,0.03359234,-0.17832004,-0.0076611526,0.0014414404,-0.19900426,0.010656047,0.025312165,0.015664242,0.0025556826,-0.050070155,0.010031128,-0.06039311,0.014591267,-0.03433222,-0.09948592,-0.03441181,-0.059614908,0.005279386,0.004610251,-0.0058188206,-0.00987195,0.002431878,0.011897041,0.0054385634,-0.0015239768,-0.02674771,0.010384856,0.009441582,-0.23564455,0.014706229,0.05309453,0.004872599,0.01200316,0.0069595925,0.07306835,-0.08020776,0.004076712,0.018007688,0.0021046798,-0.0059367297,0.03548773,0.0023404984,0.061704848,-0.005827664,0.006965488,-0.075105235,0.009538857,0.010841754,0.0,0.0";	

		String count = "";
		System.out.println(line.split("\t")[1]);
		
        for(String s: line.split("\t")[1].split(","))
        	if(count == "")
        		count = s;
        	else	
        		theta.add(Float.parseFloat(s));
	}	
	
	
}