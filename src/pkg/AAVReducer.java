package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class AAVReducer extends Reducer<AuthGram,Text,Text,Text>
{
	private MultipleOutputs<Text, Text> output;
	private static boolean singleAuthor;
	private static ArrayList<IdfWord> idfWords = new ArrayList<IdfWord>();
	
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
    	output = new MultipleOutputs<Text, Text>(context);
    	if(idfWords.size() == 0)
    	{	
    		Configuration conf = context.getConfiguration();
    		singleAuthor = conf.getBoolean("singleAuthor", false);
    		
    		String loc = conf.get("idfLocation");
    		
    		Path pt = new Path(loc);
    	    FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while(line != null)
            {
            	String[] words = line.split("\t");
            	idfWords.add( new IdfWord(words[0], Float.parseFloat(words[1])));
            	line = br.readLine();
            }
    	}
    	
    	
    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        output.close();
    }

	public void reduce(AuthGram key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{		
		/*
		 * Ug. Turns out values ARE NOT sorted in hadoop M/R. 
		 * I could do a secondary sort, but this is much easier.
		 *
		ArrayList<Text> vals = new ArrayList<Text>();
		for(Text val: values)
			vals.add(new Text(val));
		Collections.sort(vals);
		*/
		
		String aav = "";
		int outsideWords = 0;
		
		Iterator<Text> words = values.iterator();
		String[] val = words.next().toString().split(",");
		for(IdfWord idfWord: idfWords)
		{		
			if(!words.hasNext())
			{
				aav += "0,";
				continue;
			}
			
			while(idfWord.compareTo(val[0]) > 0)
			{
				if(!words.hasNext())
					break;
				val = words.next().toString().split(",");
				outsideWords++;
			}
					
			if(idfWord.compareTo(val[0]) == 0)
			{
				Float tf = Float.parseFloat(val[1]);
				aav += String.valueOf(idfWord.getIdf() * tf) + ",";
				val = words.next().toString().split(",");
			}
			else if(idfWord.compareTo(val[0]) < 0)
			{
				aav += "0,";
			}
			else
			{
				outsideWords++;
			}				
		}
		
		while(words.hasNext())
		{
			outsideWords++;
			words.next();
		}
		
		aav += String.valueOf(outsideWords);
		String fileName = key.getAuth().toString();
		if(singleAuthor)
			fileName = "AAV";
		output.write("AAV", key.getAuth(), new Text(aav), fileName);
	}
}