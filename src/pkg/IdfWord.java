package pkg;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class IdfWord implements Writable {
    private String word;
    private Float idf;

    public IdfWord() {
        set(word, idf);
    }
    
    public IdfWord(String word, Float idf) {
        set(word, idf);
    }
    
    public void set(String word, Float idf) {
        this.word = word;
        this.idf = idf;
    }
    
    public String getWord() {
        return word;
    }
    
    public float getIdf() {
    	return idf;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(word.toString());
        out.writeUTF(idf.toString());
    }
    @Override
    public void readFields(DataInput in) throws IOException {
        word = in.readUTF();
        idf = Float.valueOf(in.readUTF());
    }

    @Override
    public String toString() {
        return word + "\t" + idf.toString();
    }
    
    public int compareTo(String otherWord)
    {
    	return word.compareTo(otherWord);
    }
   
}