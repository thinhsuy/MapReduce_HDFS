package com.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrackShared {

  public static class TrackSharedMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{
	// use 'userId' and 'isShared' to store key and value
    private IntWritable trackId = new IntWritable();
    private IntWritable isShared = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// split the line into array of String
    	String[] ls = value.toString().split("[|]");
    	// set the corresponding value
    	trackId.set(Integer.parseInt(ls[LastFMConstants.TRACK_ID]));
    	isShared.set(Integer.parseInt(ls[LastFMConstants.IS_SHARED]));
    	// sending to output collector
    	context.write(trackId, isShared);
    }
  }

  public static class TrackSharedReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	// use 'result' to store the output value
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	// use 'sum' to calculate the total shared times
    	int sum=0;
    	for (IntWritable val : values) {
    		sum += val.get();
    	}
    	// set the corresponding value
    	result.set(sum);
    	// sending to output collector
    	context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "track shared");
    job.setJarByClass(TrackShared.class);
    job.setMapperClass(TrackSharedMapper.class);
    job.setCombinerClass(TrackSharedReducer.class);
    job.setReducerClass(TrackSharedReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}