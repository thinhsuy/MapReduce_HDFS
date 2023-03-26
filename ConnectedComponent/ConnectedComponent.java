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

public class ConnectedComponent {
  public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, Text>{
    public ArrayList<ArrayList<Integer>> list_of_components = new ArrayList<ArrayList<Integer>>();
    // give out index of i-th component containing value
    // whether contained, return index, else return null
    public Integer index_component_containing(Integer value){
      for (int i=0; i<list_of_components.size(); i++){
	   for (Integer comp_value: list_of_components.get(i)){
	       if (value==comp_value) return i;
	   }
       } return null;
    }

    // merge 2 paths into 1 component
    public ArrayList<Integer> add_path_to_component(ArrayList<Integer> component, ArrayList<Integer> newPath){
       for (Integer path: newPath){
	  if (component.contains(path)) continue;
	  component.add(path);
       } return component;
    }

    // generate a new list of component by a String[]
    public ArrayList<Integer> generate_new_component(String[] newPath){
       ArrayList<Integer> converted = new ArrayList<Integer>();
       for (String path: newPath) converted.add(Integer.parseInt(path));
       return converted;
    }

    public ArrayList<Integer> convert_string_to_integer(String[] listStr){
       ArrayList<Integer> result = new ArrayList<Integer>();
       for (String str: listStr) result.add(Integer.parseInt(str));
       return result;
    }

    public String get_string_by_list(ArrayList<Integer> listInt){
       String str = "";
       for (Integer val: listInt) str += String.valueOf(val) + " ";
       return str;
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split("\\s+");
      ArrayList<Integer> harshInt = convert_string_to_integer(harsh);

      boolean is_related_at_one = false;
      Integer index_contained = null;
      String contextStr = "";

      // check each value in current line whether contained by any component
      for (Integer val: harshInt){
         index_contained = index_component_containing(val);
         if (index_contained==null) continue;
         // if there is at least one component containing, merge 2 path together
         // write out its i-th component as key and CURRENT list in that component
         is_related_at_one = true;
         // assign current component containing and merge it with current path of line
         ArrayList<Integer> current_component = list_of_components.get(index_contained);
         ArrayList<Integer> new_merged_component = add_path_to_component(current_component, harshInt);
	 // set current component to the new merged component
         list_of_components.set(index_contained, new_merged_component);

         contextStr = get_string_by_list(list_of_components.get(index_contained));
         context.write(new IntWritable(index_contained), new Text(contextStr));
         return;
      }
      // when it not contained in any component, generate a new component to contain itself
      list_of_components.add(generate_new_component(harsh));
      index_contained = list_of_components.size()-1;
      contextStr = get_string_by_list(list_of_components.get(index_contained));

      context.write(new IntWritable(index_contained), new Text(contextStr));
    }
  }

  public static class TokenReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

    public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      // wirte out all the first iter of each key
      // due to because the key vale set already sorted automatically
      // which means the first result containg full path of each i-th component
      for (Text val: values){
      	 context.write(key, val);
	 break;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Patent Program");
    job.setJarByClass(ConnectedComponent.class);

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
