package pkg;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass 
{
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{	
		if (args.length != 3 ) 
		{
				System.out.printf("Usage to calculate RMSE: <jar file> <input dir> <theta dir> <alpha>\n");
				System.exit(-1);
		}
		
		
		String inputFolder = args[0];
		String thetaLocation = args[1];
		Float alpha = Float.parseFloat(args[2]);
		
		Configuration conf =new Configuration();
		conf.setFloat("alpha", alpha);
		Job job=Job.getInstance(conf);
		job.addCacheFile(new Path(thetaLocation).toUri());
	
			
		// Generate TF. 		
		job.setJarByClass(MainClass.class);
		job.setMapperClass(TheMapper.class);
		job.setReducerClass(TheReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
	
		job.setOutputFormatClass(TextOutputFormat.class);
 
		FileInputFormat.setInputPaths(job, new Path(inputFolder));
		FileOutputFormat.setOutputPath(job, new Path("/tmpOutput"));
		
		if (job.waitForCompletion(true))
		{

			// Copy /tmpOut to arg[2]

			System.exit(0);
			
		} else {
			System.exit(1);
		}
		
	}
}