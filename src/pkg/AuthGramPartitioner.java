package pkg;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class AuthGramPartitioner extends Partitioner<AuthGram, Text> 
{
    @Override
    public int getPartition(AuthGram ag, Text text, int numberOfPartitions) 
    {
       // make sure that partitions are non-negative
       return Math.abs(ag.getAuth().hashCode() % numberOfPartitions);
    }
}


/*package pkg;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
    protected CompositeKeyComparator() {
        super(AuthGram.class, true);
    }   
    
    @SuppressWarnings("rawtypes")
    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        AuthGram k1 = (AuthGram)w1;
        AuthGram k2 = (AuthGram)w2;
         
        int result = k1.getAuth().compareTo(k2.getAuth());
        if(0 == result) {
            result = -1* k1.getGram().compareTo(k2.getGram());
        }
        return result;
    }
}
*/