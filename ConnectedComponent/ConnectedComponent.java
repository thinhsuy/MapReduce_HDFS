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
  public static ArrayList<ArrayList<Integer>> list_of_components = new ArrayList<ArrayList<Integer>>();
  
  public static class TokenizerMapper extends Mapper<Object, Text, Text, Text>{
    // give out index of i-th component containing value
    // whether contained, return index, else return null
    public Integer index_component_containing(Integer value){
      for (int i=0; i<list_of_components.size(); i++){
	   for (Integer comp_value: list_of_components.get(i)){
	       if (value==comp_value) return i;
	   }
       } return null;
    }

    // convert String[] to a ArrayList<Integer>()
    public ArrayList<Integer> convert_string_to_integer(String[] listStr){
       ArrayList<Integer> result = new ArrayList<Integer>();
       for (String str: listStr) result.add(Integer.parseInt(str));
       return result;
    }
 
    // get out the string of list integer, [1,3,4] => "1 3 4" for context writing
    public String get_string_by_list(ArrayList<Integer> listInt){
       String str = "";
       for (Integer val: listInt) str += String.valueOf(val) + " ";
       return str;
    }
    
    // merge 2 paths into 1 component
    public void set_new_path_of_component_list(Integer index, ArrayList<Integer> newPath){
        // assign current component containing and merge it with current path of line
        ArrayList<Integer> current_component = list_of_components.get(index);
        for (Integer path: newPath){
	  if (current_component.contains(path)) continue;
	  current_component.add(path);
        }
	// set current component to the new merged component
        list_of_components.set(index, current_component);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      //split current line into list of values by removing consecutive spaces
      String[] harsh = (value.toString()).split("\\s+");
      // convert String[] into ArrayList<Integer>()
      ArrayList<Integer> harshInt = convert_string_to_integer(harsh);

      Integer index_contained = null; //index of i-th component that current line belong to
      String contextStr = ""; // string that used for write out notification to Reducer

      // check each value in current line whether contained by any component
      for (Integer val: harshInt){
         index_contained = index_component_containing(val);
         if (index_contained==null) continue;
         // if there is at least one component containing, merge 2 path together
         set_new_path_of_component_list(index_contained, harshInt);
         
         contextStr = get_string_by_list(list_of_components.get(index_contained));
         context.write(new Text(String.valueOf(index_contained)), new Text(contextStr));
         return;
      }

      // when it not contained in any component, generate a new component to contain itself
      list_of_components.add(harshInt);
      // means that the last component (just recently created) containing this path
      index_contained = list_of_components.size()-1;

      contextStr = get_string_by_list(list_of_components.get(index_contained));
      context.write(new Text(String.valueOf(index_contained)), new Text(contextStr));
      // every generate a new component, notice to the Reducer that Mapper just found one new component
      context.write(new Text("length"), new Text(String.valueOf(1)));
    }
  }

  public static class TokenReducer extends Reducer<Text, Text, Text, Text> {

    public void write_full_path(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
       // wirte out all the first iter of each key
       // due to because the key vale set already sorted automatically
       // which means the first result containg full path of each i-th component
        for (Text val: values){
           context.write(key, val);
           //break;
        }
    }

    public void write_total_path(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
         // above we already export the length size of total component in the key of "length"
         // finally, count all the value in key "length" to get total components
         if (key.toString().equals("length")==false) return;
         Integer sum = 0;
         for (Text val: values){
            sum += Integer.parseInt(val.toString());
         }
         context.write(key, new Text(String.valueOf(sum)));
    }

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      write_total_path(key, values, context);
      // uncomment below function to write out all the possible path in each component
      //write_full_path(key, values, context);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Connected Component Program");
    job.setJarByClass(ConnectedComponent.class);

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
