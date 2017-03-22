package pkg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

public class AuthGram implements Writable, Comparable<AuthGram>, WritableComparable<AuthGram>  {
    private Text auth;
    private Text gram;

    public AuthGram() {
        set(auth, gram);
    }
    
    public AuthGram(String auth, String gram) {
        set(new Text(auth), new Text(gram));
    }

    public AuthGram(Text auth, Text gram) {
        set(auth, gram);
    }
    
    public AuthGram(AuthGram me)
    {
    	set(me.getAuth(), me.getGram());
    }
    
    public void set(Text auth, Text gram) {
        this.auth = auth;
        this.gram = gram;
    }
    
    public Text getAuth() {
        return auth;
    }
    
    public Text getGram() {
    	return gram;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(auth.toString());
        out.writeUTF(gram.toString());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        auth = new Text(in.readUTF());
        gram = new Text(in.readUTF());
    }

    @Override
    public String toString() {
        return auth + "\t" + gram;
    }
    
    @Override
    public int compareTo(AuthGram other)
    {
        int comparedValue = auth.compareTo(other.auth);
        if (comparedValue != 0) 
        {
        	return comparedValue;
        }
        return gram.compareTo(other.gram);
    } 
}