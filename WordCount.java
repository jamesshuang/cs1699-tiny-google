import java.util.Scanner;
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

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
        String regex = "[][(){},.;!?<>%]";

      while (itr.hasMoreTokens()) {
      String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        word.set(itr.nextToken().toLowerCase().replaceAll("[^a-zA-Z ]","") +" "+ fileName);
        System.out.println(fileName);

        context.write(word, one);
      }
    }
  }

    public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text, Text> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      String[] parts = (key.toString()).split(" ");

      StringBuilder sb = new StringBuilder(parts[1] + "=>" + Integer.toString(sum));
      context.write(new Text(parts[0]), new Text(sb.toString()));
    }
  }
	
	 public static class TokenizerMapper2
       extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
    private Text word2 = new Text();
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {

    StringTokenizer itr = new StringTokenizer(value.toString());
      int count = 0;

      while (itr.hasMoreTokens()) {
        if(count == 0){
                word.set(itr.nextToken());
                count++;
        }else if(count == 1){

                word2.set(itr.nextToken());
                context.write(word, word2);
                count--;

        }
      }
      }
    }
  public static class IntSumReducer2
       extends Reducer<Text,Text,Text, Text> {

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
 Text sum = new Text("");
      StringBuilder sb = new StringBuilder();

      for (Text val : values) {
        if(sb.toString() == ""){
                sb.append(val);
        }else{
              	sb.append("," + val);
        }
      }

      context.write(key, new Text(sb.toString()));
    }
  }
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2);
    job2.setJarByClass(WordCount.class);
    job2.setJobName("Word Count");

    FileInputFormat.addInputPath(job2, new Path(args[1]));
    FileOutputFormat.setOutputPath(job2,new Path(args[1] + "_2"));
    job2.setMapperClass(TokenizerMapper2.class);
    job2.setReducerClass(IntSumReducer2.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(Text.class);
    job2.waitForCompletion(true);
    System.exit(job2.waitForCompletion(true) ? 0 : 1);

  }
}
