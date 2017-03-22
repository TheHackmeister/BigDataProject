package pkg;
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class IDFReducer extends Reducer<Text,Text,Text,Text>
{
	private MultipleOutputs<Text, Text> output;
	private static int numAuths;
	
    @Override
    public void setup(Context context) {
    	if(numAuths == 0)
    	{
    		Configuration conf = context.getConfiguration();
    		numAuths = conf.getInt("numAuthors", -1);
       	}
    	output = new MultipleOutputs<Text, Text>(context);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        output.close();
    }

	private void addCount(TreeMap<String, Integer> map, String val)
	{
		Integer count = map.getOrDefault(val, 0);
		count++;
		map.put(val, count);
	}
		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{		
		TreeMap<String, Integer> auths = new TreeMap<String, Integer>();
		for(Text val: values)
			addCount(auths, val.toString());
			
		output.write("IDF", key, new Text(String.valueOf(
				Math.log10(numAuths/(float)auths.size()))));	
	}
}