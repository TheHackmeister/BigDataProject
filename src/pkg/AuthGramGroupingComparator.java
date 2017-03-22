package pkg;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AuthGramGroupingComparator extends WritableComparator 
{
    public AuthGramGroupingComparator() {
         super(AuthGram.class, true);
    }
    @SuppressWarnings("rawtypes")
    @Override
    /**
     * This comparator controls which keys are grouped
     * together into a single call to the reduce() method
     */
    public int compare(WritableComparable wc1, WritableComparable wc2) {
    	AuthGram pair = (AuthGram) wc1;
        AuthGram pair2 = (AuthGram) wc2;
        return pair.getAuth().compareTo(pair2.getAuth());
    }
}