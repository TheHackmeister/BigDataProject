package pkg;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TFMapper extends Mapper<LongWritable, Text, Text, Text>
{	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		Pattern linePattern = Pattern.compile(".*(\\b\\w+)[^\\w]*<===>.*(\\d{4})<===>(.*)");
		String line = value.toString();
		Matcher lineMatcher = linePattern.matcher(line);
		
		if(!lineMatcher.matches())
			throw new IOException("Line did not match: " + line);
		
		String author = lineMatcher.group(1);
		String text = lineMatcher.group(3);
		
		String[] wordSplit = text.replaceAll("-{2,}", " ").split("[\\s]+");
		List<String> words = new ArrayList<>();
		
		
		for(String word: wordSplit)
		{
			String w = word.replaceAll("[^\\w\\d]", "").replaceAll("_", "").toLowerCase();  
			if(!w.equals(""))
				words.add(w);
		}
		
		// These should probably have a conditional if it's not the training data. 
		for(String word: words)
		{
			context.write(new Text(":author:" + author), new Text(word));
		}
		context.write(new Text(":AuthorCount:"), new Text(author));
	}
}