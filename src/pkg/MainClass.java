package pkg;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class MainClass {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException 
	{
		if (args.length != 2 && args.length != 3 ) 
		{
				System.out.printf("Usage to build corpus: <jar file> <input dir> <output dir>\n"
						+ "Usage to compare corpus: <jar file> <input dir> <output dir> <corpus dir>\n");
				System.exit(-1);
		}
		
		String inputFolder = args[0];
		String outputFolder = args[1];
		String corpusFolder = args[1];
		if(args.length == 3)
			corpusFolder = args[2];
		
		Configuration conf =new Configuration();
		// Generate TF. 		
		Job tfJob=Job.getInstance(conf);
		tfJob.setJarByClass(MainClass.class);
		tfJob.setMapperClass(TFMapper.class);
		tfJob.setReducerClass(TFReducer.class);
		tfJob.setOutputKeyClass(Text.class);
		tfJob.setOutputValueClass(Text.class);
		tfJob.setInputFormatClass(TextInputFormat.class);

		tfJob.setOutputFormatClass(TextOutputFormat.class);
 
		FileInputFormat.setInputPaths(tfJob, new Path(inputFolder));
		FileOutputFormat.setOutputPath(tfJob, new Path(outputFolder));
		
		LazyOutputFormat.setOutputFormatClass(tfJob, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(tfJob, "TermFrequency", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(tfJob, "AuthorCount", TextOutputFormat.class, Text.class, Text.class);
		
		tfJob.waitForCompletion(true);

		// Get the number of authors. 
		Path pt = new Path(corpusFolder + "/AuthorCount/AC-r-00000");
	    FileSystem fs = FileSystem.get(new Configuration());
        BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
        String line;
        line=br.readLine();
        if(line == null)
        	throw new IOException("Could not read number of authors.");
        Integer numAuths =Integer.parseInt(line.trim());
        conf.setInt("numAuthors", numAuths);
        
        // IDF 
		// Only need to build the IDF if we're building the corpus. 
		if(args.length == 2)
		{
		    // IDF 
	        Job idfJob=Job.getInstance(conf);
			idfJob.setJarByClass(MainClass.class);
			idfJob.setMapperClass(IDFMapper.class);
			idfJob.setReducerClass(IDFReducer.class);
			idfJob.setOutputKeyClass(Text.class);
			idfJob.setOutputValueClass(Text.class);
			idfJob.setInputFormatClass(TextInputFormat.class);
	
			idfJob.setOutputFormatClass(TextOutputFormat.class);
	 
			FileInputFormat.setInputPaths(idfJob, new Path(outputFolder + "/TermFrequency"));
			FileOutputFormat.setOutputPath(idfJob, new Path(outputFolder + "/IDF"));
			
			LazyOutputFormat.setOutputFormatClass(idfJob, TextOutputFormat.class);
			MultipleOutputs.addNamedOutput(idfJob, "IDF", TextOutputFormat.class, Text.class, Text.class);
			idfJob.waitForCompletion(true);
		}
		
		// IDF.TF
		conf.set("idfLocation", corpusFolder + "/IDF/IDF-r-00000");
		
		if(args.length == 3)
			conf.setBoolean("singleAuthor", true);
		
		Job aavJob=Job.getInstance(conf);
		aavJob.setJarByClass(MainClass.class);
		aavJob.setMapperClass(AAVMapper.class);
		aavJob.setReducerClass(AAVReducer.class);
		aavJob.setOutputKeyClass(AuthGram.class);
		aavJob.setOutputValueClass(Text.class);
		aavJob.setInputFormatClass(TextInputFormat.class);
		aavJob.setGroupingComparatorClass(AuthGramGroupingComparator.class);
		aavJob.setPartitionerClass(AuthGramPartitioner.class);

		aavJob.setOutputFormatClass(TextOutputFormat.class);
		//FileOutputFormat.
 
		FileInputFormat.setInputPaths(aavJob, new Path(outputFolder + "/TermFrequency"));
		FileOutputFormat.setOutputPath(aavJob, new Path(outputFolder + "/AAV"));
		
		LazyOutputFormat.setOutputFormatClass(aavJob, TextOutputFormat.class);
		MultipleOutputs.addNamedOutput(aavJob, "AAV", TextOutputFormat.class, Text.class, Text.class);
				
		// Compare groups
		if(args.length == 3)
		{
			aavJob.waitForCompletion(true);
			conf.set("aavLocation", outputFolder + "/AAV/AAV-r-00000");
			
			Job compJob=Job.getInstance(conf);
			compJob.setJarByClass(MainClass.class);
			compJob.setMapperClass(CompMapper.class);
			compJob.setCombinerClass(CompReducer.class);
			compJob.setReducerClass(CompReducer.class);
			compJob.setOutputKeyClass(NullWritable.class);
			compJob.setOutputValueClass(AuthFloat.class);
			compJob.setInputFormatClass(TextInputFormat.class);
			compJob.setOutputFormatClass(TextOutputFormat.class);
	 
			FileInputFormat.setInputPaths(compJob, new Path(corpusFolder + "/AAV"));
			FileOutputFormat.setOutputPath(compJob, new Path(outputFolder + "/Results"));
			
			LazyOutputFormat.setOutputFormatClass(compJob, TextOutputFormat.class);	
			//MultipleOutputs.addNamedOutput(compJob, "Comp", TextOutputFormat.class, Text.class, Text.class);
						
			System.exit(compJob.waitForCompletion(true) ? 0 : 1);
		} 
		else
		{
			System.exit(aavJob.waitForCompletion(true) ? 0 : 1);
		}
	}
}