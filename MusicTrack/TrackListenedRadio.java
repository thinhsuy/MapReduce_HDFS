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

public class TrackListenedRadio {

  public static class TrackListenedRadioMapper
       extends Mapper<Object, Text, IntWritable, IntWritable>{
	// use 'userId' and 'isRadio' to store key and value
    private IntWritable trackId = new IntWritable();
    private IntWritable isRadio = new IntWritable();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// split the line into array of String
    	String[] ls = value.toString().split("[|]");
    	// set the corresponding value
    	trackId.set(Integer.parseInt(ls[LastFMConstants.TRACK_ID]));
    	isRadio.set(Integer.parseInt(ls[LastFMConstants.RADIO]));
    	// sending to output collector
    	context.write(trackId, isRadio);
    }
  }

  public static class TrackListenedRadioReducer
       extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
	// use 'result' to store the output value
    private IntWritable result = new IntWritable();

    public void reduce(IntWritable key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	// use 'sum' to calculate the total times listened on radio
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
    Job job = Job.getInstance(conf, "track listened radio");
    job.setJarByClass(TrackListenedRadio.class);
    job.setMapperClass(TrackListenedRadioMapper.class);
    job.setCombinerClass(TrackListenedRadioReducer.class);
    job.setReducerClass(TrackListenedRadioReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}