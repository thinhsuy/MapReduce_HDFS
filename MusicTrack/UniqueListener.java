package com.mr;

import java.io.IOException;
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

public class UniqueListener {

  public static class UniqueListenersMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{
	// use 'userId' and 'trackId' to store key and value
    private IntWritable userId = new IntWritable();
    private IntWritable trackId = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// split the line into array of String
    	String[] ls = value.toString().split("[|]");
    	// set the corresponding value
    	userId.set(Integer.parseInt(ls[LastFMConstants.USER_ID]));
    	trackId.set(Integer.parseInt(ls[LastFMConstants.TRACK_ID]));
    	// sending to output collector
    	context.write(trackId, userId);
    }
  }

  public static class UniqueListenersReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	// use 'result' to store the output value
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	// use set to get the unique userId
    	Set<IntWritable> s = new HashSet<IntWritable>();
    	// run loop to process and check if the current value exists in set
    	for (IntWritable val : values) {
    		// add to set if it does not exist in set
    		if (!s.contains(val)) {
    			s.add(val);
    		}
    	}
    	// set the corresponding value
    	result.set(s.size());
    	// sending to output collector
    	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "unique listener");
    job.setJarByClass(UniqueListener.class);
    job.setMapperClass(UniqueListenersMapper.class);
//    job.setCombinerClass(UniqueListenersReducer.class);
    job.setReducerClass(UniqueListenersReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}