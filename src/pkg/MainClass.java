package pkg;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
		if (args.length != 3 && args.length != 2) 
		{
				System.out.printf("Usage to calculate RMSE: <jar file> <input dir> <theta dir> <alpha>\n");
				System.exit(-1);
		}
		
		if(args.length == 3)
		{
			String inputFolder = args[0];
			String thetaLocation = args[1];
			String tempLocation = "/tmpOutput";
			Float alpha = Float.parseFloat(args[2]);
			
			for(int i = 0; i < 20; i++)
			{
				System.out.println("Starting job: " + String.valueOf(i));
				Configuration conf =new Configuration();
			
				conf.setFloat("alpha", alpha);
				Job job=Job.getInstance(conf);
				job.addCacheFile(new Path(thetaLocation + "/part-r-00000").toUri());
					
				
				// Generate TF. 		
				job.setJarByClass(MainClass.class);
				job.setMapperClass(TheMapper.class);
				job.setReducerClass(TheReducer.class);
				job.setOutputKeyClass(Text.class);
				job.setOutputValueClass(Text.class);
				job.setInputFormatClass(TextInputFormat.class);
			
				job.setOutputFormatClass(TextOutputFormat.class);
		 
				FileInputFormat.setInputPaths(job, new Path(inputFolder));
				FileOutputFormat.setOutputPath(job, new Path(tempLocation));
				
				if (job.waitForCompletion(true))
				{
					FileSystem fs = FileSystem.get(new Configuration());
					fs.delete(new Path("/thetaBackup"), true);
					fs.rename(new Path(thetaLocation), new Path("/thetaBackup"));
					fs.rename(new Path(tempLocation), new Path(thetaLocation));
					
				} else {
					FileSystem fs = FileSystem.get(new Configuration());
					fs.delete(new Path(tempLocation + "old"), true);
					fs.rename(new Path(tempLocation), new Path(tempLocation + "old"));
					System.exit(1);
				}
			}
			System.exit(0);
		}	
		else 
		{
		
			String inputFolder = args[0];
			String outputFolder = args[1];
			Configuration conf =new Configuration();
			
			Job job=Job.getInstance(conf);
		
			job.setJarByClass(MainClass.class);
			job.setMapperClass(DataSplitterMapper.class);
			job.setReducerClass(DataSplitterReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setInputFormatClass(TextInputFormat.class);
		
			job.setOutputFormatClass(TextOutputFormat.class);
	 
			FileInputFormat.setInputPaths(job, new Path(inputFolder));
			FileOutputFormat.setOutputPath(job, new Path(outputFolder));
	
			if (job.waitForCompletion(true))
				System.exit(0);
			else
				System.exit(1);			
		}
		
	}
}