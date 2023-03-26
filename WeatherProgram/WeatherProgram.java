import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WeatherProgram {

  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    //function to get value of index i-th in an array
    private float getValueAt(String[] arr, int index){
       return Float.parseFloat(arr[index]);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split("\\s+");
      //assign date value
      String date = String.valueOf(harsh[1]);

      //check value at index 5 and 6, which present for max and min temperature, with the limitation and get conclusion
      if (getValueAt(harsh, 5)>=40.0){
	context.write(new Text(date), new Text("Hot day"));
      }
      if (getValueAt(harsh, 6)<=10){
        context.write(new Text(date), new Text("Cold day"));
      }
    }
  }

  public static class TokenReducer extends Reducer<Text, IntWritable, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      //get out all the values and write it to console
      String temperature = values.toString();
      context.write(key, new Text(temperature));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "temperature");
    job.setJarByClass(WeatherProgram.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(TokenReducer.class);
    job.setReducerClass(TokenReducer.class);

    //setting new output format for Mapper and Reducer
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
