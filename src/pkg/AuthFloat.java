package pkg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AuthFloat implements Writable, Comparable<AuthFloat> {
    private Text auth;
    private Float val;

    public AuthFloat() {
        set(auth, val);
    }
    
    public AuthFloat(String auth, Float val) {
        set(new Text(auth), val);
    }

    public AuthFloat(Text auth, Float val) {
        set(auth, val);
    }
    
    public AuthFloat(AuthFloat me)
    {
    	set(me.getAuth(), me.getVal());
    }
    
    public void set(Text auth, Float val) {
        this.auth = auth;
        this.val = val;
    }
    
    public Text getAuth() {
        return auth;
    }
    
    public Float getVal() {
    	return val;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(auth.toString());
        out.writeUTF(val.toString());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        auth = new Text(in.readUTF());
        val = Float.valueOf(in.readUTF());
    }

    @Override
    public String toString() {
        return auth + "\t" + val.toString();
    }
    
    @Override
    public int compareTo(AuthFloat otherVal)
    {
    	return otherVal.getVal().compareTo(val);
    } 
}