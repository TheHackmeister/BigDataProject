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

public class DataSplitterReducer extends Reducer<Text,Text,Text,Text>
{
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
	{			
		for(Text value: values)
		{
			context.write(new Text(""), value);
		}
	}
}