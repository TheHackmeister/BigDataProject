package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class DataSplitterMapper extends TheMapper
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		// Data structure: 
		// [0]MatchId, [1]Match Version, [2]Region, [3/4]Match Type/Something?, [5]Season, [6]Queue Type, [7] Unimportant, 
		// [8]Bans, [9]Team 1 champs, [10]Team -1 champs, [11]Team 1 spells, [12]Team -1 spells, [13]Winner.
		
		String features = "F";
		String reducer = "R";
		
		// Split input into something real.
		
		String[] line = value.toString().split(",");
		if(Integer.parseInt(line[13]) < 101)
			features = "1,";
		else 
			features = "-1,";
		//features = line[13] + ',';	
		//features += line[3] + ",";
		
		features += champsToString(line[8], line[9]);
		//features += line[7];
		
		// Taking the last value of the match ID as the key. This way it splits it into 10 about even splits. 
		//context.write(new Text(line[0].substring(line[0].length() - 1)),  new Text(features));
		context.write(new Text("One"),  new Text(features.substring(0, features.length() -1)));
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		ArrayList<Integer> a = new ArrayList<Integer>();
		a.add(1);
		a.add(2);
		System.out.println(a.toString());
		for(Integer i: a)
		{
			i = 5;
		}
		System.out.println(a.toString());

	}	
}