package pkg;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IDFMapper extends Mapper<LongWritable, Text, Text, Text>
{	
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String line = value.toString();
		Pattern linePattern = Pattern.compile("(.*)\t(.*)\t(.*)");
		Matcher lineMatcher = linePattern.matcher(line);
		
		if(!lineMatcher.matches())
			throw new IOException("Line did not match: " + line);
		
		
		String author = lineMatcher.group(1);
		String gram = lineMatcher.group(2);
				
		context.write(new Text(gram), new Text(author));
	}
}