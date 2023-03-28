import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSalary {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, FloatWritable>{

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split("\\s+");
      String category = harsh[2].toString(); //get current categorical role
      float salary = Float.parseFloat(harsh[3]); //get current salary related to role

      context.write(new Text(category), new FloatWritable(salary));
    }
  }

  public static class TokenReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

    public void reduce(Text key, Iterable<FloatWritable> values, Context context) throws IOException, InterruptedException {
      float sum = 0;
      float total = 0;
      // sum all value in specific key then divide it into total of its iterable values
      for (FloatWritable val: values){
         sum+=val.get();
         total+=1;
      }
      context.write(key, new FloatWritable(sum/total));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Average Salary Program");
    job.setJarByClass(AverageSalary.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TokenReducer.class);
    job.setReducerClass(TokenReducer.class);

    //setting new output format for Mapper and Reducer
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(FloatWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(FloatWritable.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
