package pkg;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class TFReducer extends Reducer<Text,Text,Text,Text>
{
	private MultipleOutputs<Text, Text> output;
	
    @Override
    public void setup(Context context) {
        output = new MultipleOutputs<Text, Text>(context);
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        output.close();
    }
    
	private void addCount(TreeMap<String, Integer> map, String val) throws IOException, InterruptedException
	{
		Integer count = map.getOrDefault(val, 0);
		count++;
		map.put(val, count);
	}
		
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{
		// May need to make sure key exists.
		
		if(key.equals(new Text(":AuthorCount:")))
		{		
			HashMap<Text, Integer> auths = new HashMap<Text, Integer>();
			for(Text auth: values)
				auths.put(auth, null);
			if(!(auths.size() > 1))
				return;
			output.write("AuthorCount", new Text(String.valueOf(auths.size())), new Text(""), "AuthorCount/AC");		
			return;
		}
			
		String auth = key.toString().replace(":author:", "");
		TreeMap<String, Integer> grams = new TreeMap<String, Integer>();
		for(Text gram: values)
			addCount(grams, gram.toString());
			
		float maxFrequency = (float)grams.firstEntry().getValue();
		for(Map.Entry<String, Integer> word: grams.entrySet())
			if(word.getValue() > maxFrequency)
				maxFrequency = (float)word.getValue();
		for(Map.Entry<String, Integer> word: grams.entrySet())
			output.write("TermFrequency", new Text(auth), new Text(word.getKey() + "\t" + String.valueOf(0.5 + 0.5 * (word.getValue()/maxFrequency))), "TermFrequency/TF");
	}
}