import java.io.IOException;
import java.util.StringTokenizer;
import java.io.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  public static class TokenizerMapper extends Mapper<Object, Text, Text, IIElement>{
    private final static IIElement element = new IIElement;
    private Text word = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
	    String regex = "[][(){},.;!?<>%]";
	
	    //output of map is word mapped to a IIElement, which contains filename and frequency of word
      while (itr.hasMoreTokens()) {
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
	      word.set(itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z ]",""));
        element.setFilename(fileName);
        element.setFreq(1); 
	      context.write(word, element);
      }
    }
  }

  //reducer is not complete
  public static class IntSumReducer extends Reducer<Text,IIElement,Text,II> {
    private IntWritable result = new IntWritable();
    public void reduce(Text key, Iterable<IIElement> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      for (IIElement val : values) {
        sum += val.getFreq();
      }
      result.set(sum);
      context.write(key, result);
    }
  }
  
  /*
   * Custome object to store each element in our inverted Index
   */
  public static class IIElement implements WritableComparable<IIElement> {
    private String filename;
    private int freq;
    
    public void setFilename(String filename) {
      this.filename = filename;
    }
    
    public void setFreq(int freq) {
      this.freq = freq;
    }
    
    public int getFreq() {
      return freq;
    }
    
    public String getFilename() {
      return filename;
    }
    
    public void write(DataOutput out) throws IOException {
      out.writeUTF(filename);
      out.writeInt(freq);
    }
    
    public void readFields(DataInput in) throws IOException {
      filename = in.readUTF();
      freq = in.readInt();
    }
    
    /*
     * Compares two elements in the Inverted Index
     *
     * @param o the element we want to compare this element to
     * @return postive int if this object is greater than object being passed, negative int if
     * less than, and zero is two objects are equals
     */
    public int compareTo(IIElement o) {
      int thisValue = this.freq;
      int thatValue = o.freq;
      return (thisValue < thatValue ? -1 : (thisValue==thatValue ? 0 : 1));
    }
  }
  
  //incomplete
  public static class II implements WritableComparable<II> {
    private ArrayList<IIElement> invertedIndex;
    
    public void write(DataOutput out) throws IOException {
      ;
    }
    
    public void readFields(DataInput in) throws IOException {
      ;
    }
  }
  
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
