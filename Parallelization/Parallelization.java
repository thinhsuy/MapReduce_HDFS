import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Parallelization {
  //public static ArrayList<ArrayList<Integer>> final_matrix = new ArrayList<ArrayList<Ingeger>>();
  public static Integer size_of_matrix = 0;
  public static Integer current_row = -1;

  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split("\\s+");
      size_of_matrix = harsh.length;
      current_row+=1;
      if (current_row>=size_of_matrix) current_row=0;

      //assign for each ids in data as value of 1
      context.write(new IntWritable(current_row), new Text(value.toString()));
    }
  }
  //									    >> change this for another type of value class output
  public static class TokenReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      ArrayList<Integer> result = new ArrayList<Integer>();
      for (Text iter: values){
         String[] harsh = (iter.toString()).split("\\s+");
         if (result.size()==0){
             for (int i=0; i<harsh.length; i++){
                  result.add(Integer.parseInt(harsh[i]));
             }
         } else {
             for (int i=0; i<harsh.length; i++){
                 result.set(i, result.get(i)+Integer.parseInt(harsh[i]));
             }
         }
      }
      String resultStr = "";
      for (Integer val: result) resultStr += String.valueOf(val) + " ";
      context.write(key, new Text(resultStr.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Parellelization Program");
    job.setJarByClass(Parallelization.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TokenReducer.class);
    job.setReducerClass(TokenReducer.class);

    //setting new output format for Mapper and Reducer
    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
