package pkg;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DataSplitterMapper extends TheMapper
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		
		// Data structure: 
		// [0]MatchId, [1]Match Version, [2]Region, [3/4]Match Type/Something?, [5]Season, [6]Queue Type, [7] Unimportant, 
		// [8]Bans, [9]Team 1 champs, [10]Team -1 champs, [11]Team 1 spells, [12]Team -1 spells, [13]Winner.
		
		String features = "";
		
		// Split input into something real.
		String[] line = value.toString().split(",");
		if(Integer.parseInt(line[13]) < 101)
			features = "1,";
		else 
			features = "-1,";
		
		features += champsToString(line[8], line[9]);

		context.write(new Text("One"),  new Text(features.substring(0, features.length() -1)));
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
	}	
}