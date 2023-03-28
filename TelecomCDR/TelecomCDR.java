package com.mr;

import java.io.IOException;
import java.util.*;
import java.text.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TelecomCDR {
	/*
	 * Get the millisecond from datetime
	 * Args:
	 * 		date: a string, the datetime we need to parse
	 * Returns:
	 * 		result: the millisecond after parsing
	 */
	public static long getMils(String date) {
		SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd HH:mm:ss");
		Date dateFrm = null;
		try {
			dateFrm = format.parse(date);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return dateFrm.getTime();
	}
	
	/*
	 * Count the duration between two datetime in minute
	 * Args:
	 * 		date1: a string, the call start time
	 * 		date2: a string, the call end time
	 * Returns:
	 * 		result: the duration in minute
	 */
	public static LongWritable getDuration(String date1, String date2) {
		return new LongWritable((getMils(date2) - getMils(date1)) / (1000 * 60));
		
	}
	
	public static class TelecomCDRMapper
       extends Mapper<Object, Text, Text, LongWritable>{
	// use 'phone' to store the output key 
    private Text phone = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	// split the line into array of String
    	String[] ls = value.toString().split("[|]");
    	// set the corresponding value
    	phone.set(ls[CDRConstants.fromPhoneNumber]);
    	// check if it is a long distance (std) call
    	if (Integer.parseInt(ls[CDRConstants.STDFlag]) == 1) {
    		// sending to output collector
    		context.write(phone, getDuration(ls[CDRConstants.callStartTime], ls[CDRConstants.callEndTime]));
    	}
    }
  }

  public static class TelecomCDRReducer
       extends Reducer<Text,LongWritable,Text,LongWritable> {
	// use 'result' to store the output value
    private LongWritable result = new LongWritable();

    public void reduce(Text key, Iterable<LongWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      // use 'sum' to calculate the total duration in minute
      long sum = 0;
      // run for loop to process
      for (LongWritable val : values) {
        sum += val.get();
      }
      // set the corresponding value
      result.set(sum);
      // check if the total duration is more than 60 minutes
      if (sum >= 60) {
    	  // sending to output collector
    	  context.write(key, result);
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "telecom cdr");
    job.setJarByClass(TelecomCDR.class);
    job.setMapperClass(TelecomCDRMapper.class);
    job.setCombinerClass(TelecomCDRReducer.class);
    job.setReducerClass(TelecomCDRReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}