package pkg;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class DataSplitterMapper extends TheMapper
{
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		context.write(new Text("One"),  new Text(translateLine(value)));
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
	}	
}