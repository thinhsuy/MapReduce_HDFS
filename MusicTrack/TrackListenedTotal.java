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

public class TrackListenedTotal {

  public static class TrackListenedTotalMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{
	// use 'userId' to store the output key
    private IntWritable trackId = new IntWritable();
    private final IntWritable one = new IntWritable(1);

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// split the line into array of String
    	String[] ls = value.toString().split("[|]");
    	// set the corresponding value
    	trackId.set(Integer.parseInt(ls[LastFMConstants.TRACK_ID]));
    	// sending to output collector
    	context.write(trackId, one);
    }
  }

  public static class TrackListenedTotalReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	// use 'userId' and 'isRadio' to store key and value
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	// use 'sum' to calculate the total listened times
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
    Job job = Job.getInstance(conf, "track listened total");
    job.setJarByClass(TrackListenedTotal.class);
    job.setMapperClass(TrackListenedTotalMapper.class);
    job.setCombinerClass(TrackListenedTotalReducer.class);
    job.setReducerClass(TrackListenedTotalReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}