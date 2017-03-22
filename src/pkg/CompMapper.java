package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;

public class CompMapper extends Mapper<LongWritable, Text, NullWritable, AuthFloat>
{	
	private static ArrayList<Float> aav = new ArrayList<Float>();
	private static Integer extraWords;
	
    @Override
    public void setup(Context context) throws IOException {
		if(aav.size() == 0)
		{	
			Configuration conf = context.getConfiguration();
			String loc = conf.get("aavLocation");
			Path pt = new Path(loc);
		    FileSystem fs = FileSystem.get(new Configuration());
	        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
	        String[] input;
	        input=br.readLine().split("\t");
	        String[] values = input[1].split(",");
	        
	        extraWords = Integer.parseInt(values[values.length - 1]);
	        // I set this to 0 rather than delete it. 
	        values[values.length-1] = "0";
	        for(String val: values)
	         	aav.add(Float.parseFloat(val));
		}
    }
    
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] line = value.toString().split("\t");
		String currentAuth = line[0];
		String[] vals = line[1].split(",");

		ArrayList<Float> currentAAV = new ArrayList<Float>();
		for(String val: vals)
			currentAAV.add(Float.parseFloat(val));
		
		if(currentAAV.size() != aav.size())
			throw new IOException("The size of AAVs did not match. Weird. Fix it and try again?");
		
		double times = 0;
		double magMystery = 0;
		double magCurrent = 0;
		
		for(int i = 0; i < currentAAV.size(); i++)
		{
			magMystery += aav.get(i) * aav.get(i);
			magCurrent += currentAAV.get(i) * currentAAV.get(i);
			times += aav.get(i) * currentAAV.get(i);			
		}
		
		Configuration conf = context.getConfiguration();
		int numAuths = conf.getInt("numAuthors", 1);
		magMystery += Math.log10(numAuths/1) * extraWords;
				
		magMystery = Math.sqrt(magMystery);
		magCurrent = Math.sqrt(magCurrent);
		
		context.write(NullWritable.get(), new AuthFloat(currentAuth, (float) (times/(magMystery*magCurrent))));
	}
}