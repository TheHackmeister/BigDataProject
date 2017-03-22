package pkg;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class CompReducer extends Reducer<NullWritable,AuthFloat,NullWritable,AuthFloat>
{
	public void reduce(NullWritable key, Iterable<AuthFloat> values, Context context) throws IOException, InterruptedException 
	{	
		ArrayList<AuthFloat> auths = new ArrayList<AuthFloat>();
		for(AuthFloat val: values)
		{
			auths.add(new AuthFloat(val));
		}
		Collections.sort(auths);
		
		for(int i = 0; i < Math.min(auths.size(), 10); i++)
		{
			context.write(NullWritable.get(), auths.get(i));
		}
	}
}